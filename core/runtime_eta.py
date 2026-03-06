"""
Runtime ETA estimation and telemetry recording utilities.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence
import math

from core.runtime_store import (
    fetch_prediction_errors_percent,
    fetch_probe_elapsed_ms,
    fetch_query_elapsed_ms,
    initialize_runtime_store,
    insert_probe_runtime_event,
    insert_query_runtime_event,
)
from core.sparql import post_sparql_with_debug


MIN_BUCKET_SAMPLES = 12
CALIBRATION_MIN_SAMPLES = 30
CALIBRATION_P50_APE_TARGET = 25.0

CONFIDENCE_ORDER = {"low": 1, "medium": 2, "high": 3}
CONFIDENCE_BY_ORDER = {v: k for k, v in CONFIDENCE_ORDER.items()}

ANALYSIS_STEP_LABELS: dict[str, list[str]] = {
    "upstream": [
        "Step 1: PFAS Samples",
        "Step 2: Upstream Flowlines",
        "Step 3: Upstream Facilities",
    ],
    "downstream": [
        "Step 1: Facilities",
        "Step 2: Downstream Streams",
        "Step 3: Downstream Samples",
    ],
    "near_facilities": [
        "Step 1: Facilities",
        "Step 2: Nearby Samples",
    ],
    "sockg_sites": [
        "Step 1: SOCKG Locations",
        "Step 2: SOCKG Nearby Facilities",
    ],
    "aquifer_wells": [
        "Step 1: Samples, Aquifers & Wells",
    ],
}

# Seeded priors in milliseconds for (low, mid, high).
STEP_PRIORS_MS: dict[str, dict[str, tuple[float, float, float]]] = {
    "upstream": {
        "Step 1: PFAS Samples": (3000.0, 5000.0, 8000.0),
        "Step 2: Upstream Flowlines": (2500.0, 4000.0, 7000.0),
        "Step 3: Upstream Facilities": (2500.0, 4000.0, 7000.0),
    },
    "downstream": {
        "Step 1: Facilities": (1500.0, 2500.0, 5000.0),
        "Step 2: Downstream Streams": (55000.0, 75000.0, 95000.0),
        "Step 3: Downstream Samples": (35000.0, 50000.0, 70000.0),
    },
    "near_facilities": {
        "Step 1: Facilities": (1200.0, 2000.0, 4500.0),
        "Step 2: Nearby Samples": (4000.0, 7000.0, 12000.0),
    },
    "sockg_sites": {
        "Step 1: SOCKG Locations": (2000.0, 4000.0, 7000.0),
        "Step 2: SOCKG Nearby Facilities": (4000.0, 7000.0, 12000.0),
    },
    "aquifer_wells": {
        "Step 1: Contaminated Samples & Aquifers": (5000.0, 12000.0, 25000.0),
        "Step 2: Connected Wells": (2000.0, 5000.0, 10000.0),
    },
}

DEFAULT_PRIOR_MS = (15000.0, 30000.0, 45000.0)


@dataclass(frozen=True)
class EtaRequest:
    analysis_key: str
    step_labels: list[str]
    endpoint_key: str = "federation"
    region_level: str = "all"
    state_code: Optional[str] = None
    naics_prefix2: Optional[str] = None
    include_nondetects: bool = False
    has_substance_filter: bool = False
    has_material_filter: bool = False
    conc_bin: str = "50-500"
    probe_elapsed_ms: Optional[float] = None
    probe_status: Optional[str] = None


@dataclass(frozen=True)
class StepEta:
    label: str
    low_s: float
    mid_s: float
    high_s: float
    confidence: str
    sample_size: int


@dataclass(frozen=True)
class EtaResult:
    total_low_s: float
    total_mid_s: float
    total_high_s: float
    confidence: str
    step_estimates: list[StepEta]
    source: str
    estimate_label: str
    similar_runs: int


def with_probe_result(
    request: EtaRequest,
    probe_elapsed_ms: Optional[float],
    probe_status: Optional[str],
) -> EtaRequest:
    """Return a copy of request enriched with preflight probe metadata."""
    return EtaRequest(
        analysis_key=request.analysis_key,
        step_labels=list(request.step_labels),
        endpoint_key=request.endpoint_key,
        region_level=request.region_level,
        state_code=request.state_code,
        naics_prefix2=request.naics_prefix2,
        include_nondetects=request.include_nondetects,
        has_substance_filter=request.has_substance_filter,
        has_material_filter=request.has_material_filter,
        conc_bin=request.conc_bin,
        probe_elapsed_ms=probe_elapsed_ms,
        probe_status=probe_status,
    )


def estimate_eta_with_probe(
    request: EtaRequest,
    probe_timeout_sec: int = 8,
) -> tuple[EtaRequest, EtaResult, Optional[float], str]:
    """
    Run a preflight probe, apply it to request, and estimate ETA.
    """
    probe_elapsed_ms, probe_status = run_preflight_probe(request.endpoint_key, timeout_sec=probe_timeout_sec)
    adjusted_request = with_probe_result(request, probe_elapsed_ms=probe_elapsed_ms, probe_status=probe_status)
    return adjusted_request, estimate_eta(adjusted_request), probe_elapsed_ms, probe_status


def total_elapsed_seconds(executed_queries: Sequence[Mapping[str, Any]]) -> float:
    """Sum `elapsed_ms` values from executed query records into seconds."""
    return sum(float(q.get("elapsed_ms") or 0.0) for q in executed_queries) / 1000.0


def build_eta_summary(actual_total_s: float, eta: EtaResult) -> dict[str, Any]:
    """Create a serializable ETA summary payload for session state."""
    return {
        "actual_total_s": float(actual_total_s),
        "pred_low_s": float(eta.total_low_s),
        "pred_mid_s": float(eta.total_mid_s),
        "pred_high_s": float(eta.total_high_s),
        "confidence": eta.confidence,
        "estimate_label": eta.estimate_label,
        "similar_runs": int(eta.similar_runs),
    }


def region_level_from_code(region_code: Optional[str]) -> str:
    code = (region_code or "").strip()
    if not code:
        return "all"
    if len(code) <= 2:
        return "state"
    if len(code) <= 5:
        return "county"
    return "subdivision"


def concentration_bin(min_conc: float, max_conc: float) -> str:
    low = max(float(min_conc or 0), 0.0)
    high = max(float(max_conc or 0), low)
    if high <= 10:
        return "0-10"
    if high <= 50:
        return "10-50"
    if high <= 500:
        return "50-500"
    return "500+"


def naics_prefix2_from_code(naics_code: Optional[str]) -> Optional[str]:
    raw = "".join(ch for ch in str(naics_code or "") if ch.isdigit())
    return raw[:2] if len(raw) >= 2 else None


def list_analysis_steps(analysis_key: str) -> list[str]:
    return list(ANALYSIS_STEP_LABELS.get(analysis_key, []))


def build_eta_request(
    analysis_key: str,
    region_code: Optional[str],
    state_code: Optional[str],
    min_conc: float,
    max_conc: float,
    include_nondetects: bool,
    naics_prefix2: Optional[str] = None,
    has_substance_filter: bool = False,
    has_material_filter: bool = False,
    endpoint_key: str = "federation",
) -> EtaRequest:
    """
    Build a standardized ETA request payload used by analysis UIs.
    """
    return EtaRequest(
        analysis_key=analysis_key,
        step_labels=list_analysis_steps(analysis_key),
        endpoint_key=endpoint_key,
        region_level=region_level_from_code(region_code),
        state_code=state_code,
        naics_prefix2=naics_prefix2,
        include_nondetects=include_nondetects,
        has_substance_filter=has_substance_filter,
        has_material_filter=has_material_filter,
        conc_bin=concentration_bin(min_conc, max_conc),
    )


def infer_runtime_status(error_message: Optional[str]) -> str:
    msg = str(error_message or "").strip().lower()
    if not msg:
        return "success"
    if "timeout" in msg:
        return "timeout"
    return "error"


def _safe_float(value: Optional[float], default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    p = _clamp(percentile, 0.0, 100.0)
    rank = (len(ordered) - 1) * (p / 100.0)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    weight = rank - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight


def _round_total_seconds(value: float, unit_seconds: int) -> float:
    rounded = round(value / unit_seconds) * unit_seconds
    return float(max(unit_seconds, rounded))


def _downgrade_confidence(confidence: str) -> str:
    level = CONFIDENCE_ORDER.get(confidence, 1)
    return CONFIDENCE_BY_ORDER[max(1, level - 1)]


def _confidence_from_samples(sample_size: int, used_prior: bool) -> str:
    if used_prior or sample_size < MIN_BUCKET_SAMPLES:
        return "low"
    if sample_size >= 40:
        return "high"
    if sample_size >= 20:
        return "medium"
    return "low"


def _prior_ms(analysis_key: str, step_label: str) -> tuple[float, float, float]:
    return STEP_PRIORS_MS.get(analysis_key, {}).get(step_label, DEFAULT_PRIOR_MS)


def _bucket_filters(request: EtaRequest) -> list[tuple[str, dict[str, object]]]:
    level_a = {
        "region_level": request.region_level,
        "state_code": request.state_code,
        "naics_prefix2": request.naics_prefix2,
        "include_nondetects": int(request.include_nondetects),
        "has_substance_filter": int(request.has_substance_filter),
        "has_material_filter": int(request.has_material_filter),
        "conc_bin": request.conc_bin,
    }
    level_b = dict(level_a)
    level_b.pop("state_code", None)

    level_c = {"region_level": request.region_level}
    level_d: dict[str, object] = {}
    return [("A", level_a), ("B", level_b), ("C", level_c), ("D", level_d)]


def _step_quantiles_ms(request: EtaRequest, step_label: str) -> tuple[float, float, float, int, str]:
    for bucket_name, filters in _bucket_filters(request):
        values = fetch_query_elapsed_ms(
            analysis_key=request.analysis_key,
            step_label=step_label,
            endpoint_key=request.endpoint_key,
            filters=filters,
            statuses=("success",),
            days=90,
        )
        if len(values) >= MIN_BUCKET_SAMPLES:
            return (
                _percentile(values, 25.0),
                _percentile(values, 50.0),
                _percentile(values, 75.0),
                len(values),
                bucket_name,
            )

    low, mid, high = _prior_ms(request.analysis_key, step_label)
    return low, mid, high, 0, "prior"


def _probe_factor(request: EtaRequest) -> float:
    if request.probe_status != "success" or request.probe_elapsed_ms is None:
        return 1.0
    baseline_values = fetch_probe_elapsed_ms(
        endpoint_key=request.endpoint_key,
        statuses=("success",),
        days=30,
    )
    if not baseline_values:
        return 1.0
    baseline_ms = _percentile(baseline_values, 50.0)
    if baseline_ms <= 0:
        return 1.0
    return _clamp(request.probe_elapsed_ms / baseline_ms, 0.7, 1.8)


def _accuracy_label(request: EtaRequest) -> str:
    errors = fetch_prediction_errors_percent(
        analysis_key=request.analysis_key,
        endpoint_key=request.endpoint_key,
        days=30,
    )
    if len(errors) < CALIBRATION_MIN_SAMPLES:
        return "Rough estimate (calibrating)"
    if _percentile(errors, 50.0) <= CALIBRATION_P50_APE_TARGET:
        return "Estimated time"
    return "Rough estimate (calibrating)"


def estimate_eta(request: EtaRequest) -> EtaResult:
    """
    Estimate total runtime as a low-mid-high range with confidence.
    """
    initialize_runtime_store()
    factor = _probe_factor(request)

    step_estimates: list[StepEta] = []
    bucket_sources: list[str] = []
    for step_label in request.step_labels:
        low_ms, mid_ms, high_ms, sample_size, source = _step_quantiles_ms(request, step_label)
        bucket_sources.append(source)
        used_prior = source == "prior"
        confidence = _confidence_from_samples(sample_size, used_prior)

        if request.probe_status in {"timeout", "error"}:
            confidence = _downgrade_confidence(confidence)

        step_estimates.append(
            StepEta(
                label=step_label,
                low_s=max(1.0, (low_ms * factor) / 1000.0),
                mid_s=max(1.0, (mid_ms * factor) / 1000.0),
                high_s=max(1.0, (high_ms * factor) / 1000.0),
                confidence=confidence,
                sample_size=sample_size,
            )
        )

    if not step_estimates:
        return EtaResult(
            total_low_s=0.0,
            total_mid_s=0.0,
            total_high_s=0.0,
            confidence="low",
            step_estimates=[],
            source="priors",
            estimate_label="Rough estimate (calibrating)",
            similar_runs=0,
        )

    total_low = sum(s.low_s for s in step_estimates)
    total_mid = sum(s.mid_s for s in step_estimates)
    total_high = sum(s.high_s for s in step_estimates)
    round_unit = 1 if total_mid < 20 else 5
    total_low = _round_total_seconds(total_low, round_unit)
    total_mid = _round_total_seconds(total_mid, round_unit)
    total_high = _round_total_seconds(total_high, round_unit)

    confidence_rank = min(CONFIDENCE_ORDER.get(s.confidence, 1) for s in step_estimates) if step_estimates else 1
    overall_confidence = CONFIDENCE_BY_ORDER[confidence_rank]

    if all(src == "prior" for src in bucket_sources):
        source = "priors"
    elif any(src == "prior" for src in bucket_sources):
        source = "mixed"
    else:
        source = "historical"

    non_zero_samples = [s.sample_size for s in step_estimates if s.sample_size > 0]
    similar_runs = min(non_zero_samples) if non_zero_samples else 0

    return EtaResult(
        total_low_s=total_low,
        total_mid_s=total_mid,
        total_high_s=total_high,
        confidence=overall_confidence,
        step_estimates=step_estimates,
        source=source,
        estimate_label=_accuracy_label(request),
        similar_runs=similar_runs,
    )


def estimate_remaining_range(
    eta_result: EtaResult,
    completed_step_labels: Sequence[str],
    observed_elapsed_s: float,
) -> tuple[float, float]:
    """
    Estimate remaining low/high seconds after some steps are complete.
    """
    completed = set(completed_step_labels)
    predicted_completed_mid = sum(s.mid_s for s in eta_result.step_estimates if s.label in completed)
    remaining_low = sum(s.low_s for s in eta_result.step_estimates if s.label not in completed)
    remaining_high = sum(s.high_s for s in eta_result.step_estimates if s.label not in completed)

    if remaining_high <= 0:
        return 0.0, 0.0

    scale = 1.0
    if predicted_completed_mid > 0 and observed_elapsed_s > 0:
        scale = _clamp(observed_elapsed_s / predicted_completed_mid, 0.6, 1.8)

    return max(0.0, remaining_low * scale), max(0.0, remaining_high * scale)


def run_preflight_probe(endpoint_key: str = "federation", timeout_sec: int = 8) -> tuple[Optional[float], str]:
    """
    Run a lightweight ASK probe and return (elapsed_ms_or_none, status).
    """
    query = "ASK { ?s ?p ?o }"
    _, error, debug = post_sparql_with_debug(endpoint_key, query, timeout=timeout_sec)
    elapsed_ms = _safe_float(debug.get("elapsed_ms"), default=0.0)
    status = infer_runtime_status(error)

    insert_probe_runtime_event(
        {
            "endpoint_key": endpoint_key,
            "elapsed_ms": elapsed_ms,
            "status": status,
        }
    )

    if status == "success":
        return elapsed_ms, status
    return None, status


def record_probe_runtime(
    endpoint_key: str,
    elapsed_ms: Optional[float],
    status: str,
) -> None:
    """Record one probe runtime event."""
    insert_probe_runtime_event(
        {
            "endpoint_key": endpoint_key,
            "elapsed_ms": _safe_float(elapsed_ms, default=0.0),
            "status": status,
        }
    )


def record_query_runtime(
    request: EtaRequest,
    step_label: str,
    status: str,
    elapsed_ms: Optional[float],
    row_count: Optional[int] = None,
    predicted_step: Optional[StepEta] = None,
) -> None:
    """Record one executed query runtime for telemetry and calibration."""
    elapsed = _safe_float(elapsed_ms, default=0.0)
    if elapsed <= 0:
        return

    insert_query_runtime_event(
        {
            "analysis_key": request.analysis_key,
            "step_label": step_label,
            "endpoint_key": request.endpoint_key,
            "status": status,
            "elapsed_ms": elapsed,
            "row_count": int(row_count) if row_count is not None else None,
            "region_level": request.region_level,
            "state_code": request.state_code,
            "naics_prefix2": request.naics_prefix2,
            "include_nondetects": request.include_nondetects,
            "has_substance_filter": request.has_substance_filter,
            "has_material_filter": request.has_material_filter,
            "conc_bin": request.conc_bin,
            "pred_low_ms": (predicted_step.low_s * 1000.0) if predicted_step else None,
            "pred_mid_ms": (predicted_step.mid_s * 1000.0) if predicted_step else None,
            "pred_high_ms": (predicted_step.high_s * 1000.0) if predicted_step else None,
            "pred_confidence": predicted_step.confidence if predicted_step else None,
        }
    )


def record_executed_query_batch(
    request: EtaRequest,
    executed_queries: Iterable[Mapping[str, Any]],
    step_eta_by_label: Mapping[str, StepEta] | None = None,
) -> None:
    """
    Record runtime telemetry for a collection of executed query debug records.
    """
    eta_lookup = step_eta_by_label or {}
    for query_info in executed_queries:
        step_label = str(query_info.get("label") or "")
        record_query_runtime(
            request=request,
            step_label=step_label,
            status=infer_runtime_status(query_info.get("error")),
            elapsed_ms=query_info.get("elapsed_ms"),
            row_count=query_info.get("row_count"),
            predicted_step=eta_lookup.get(step_label),
        )
