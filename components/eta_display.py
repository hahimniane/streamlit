"""
Shared UI rendering for query runtime ETA.
"""
from __future__ import annotations

from typing import Any, Mapping, Optional
import streamlit as st

from core.runtime_eta import EtaResult


def _format_duration(seconds: float) -> str:
    total = max(0, int(round(float(seconds))))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def render_eta_card(eta: EtaResult, title: str = "Estimated wait") -> None:
    """Render a compact ETA card in the current container."""
    st.info(f"**{title}**: {_format_duration(eta.total_low_s)} - {_format_duration(eta.total_high_s)}")
    if eta.similar_runs > 0:
        st.caption(
            f"{eta.estimate_label} | Confidence: {eta.confidence.title()} | "
            f"Based on {eta.similar_runs} similar runs"
        )
    else:
        st.caption(
            f"{eta.estimate_label} | Confidence: {eta.confidence.title()} | "
            "Based on seeded runtime priors"
        )


def render_simple_eta(eta: EtaResult) -> None:
    """Render a single, user-friendly ETA line for query wait time."""
    low_min = max(1, int(round(float(eta.total_low_s) / 60.0)))
    high_min = max(low_min, int(round(float(eta.total_high_s) / 60.0)))

    if low_min == high_min:
        label = f"~{low_min} minute" if low_min == 1 else f"~{low_min} minutes"
    else:
        label = f"~{low_min}-{high_min} minutes"

    st.info(f"Estimated query time: {label}")


def render_probe_adjustment(probe_status: str, probe_elapsed_ms: Optional[float]) -> None:
    """Render probe status text."""
    if probe_status == "success":
        st.caption(f"Preflight probe complete ({int(round(probe_elapsed_ms or 0.0))} ms).")
    else:
        st.caption("Preflight probe unavailable; using baseline estimate.")


def render_remaining_eta(
    placeholder,
    elapsed_s: float,
    remaining_low_s: float,
    remaining_high_s: float,
) -> None:
    """Render elapsed/remaining estimate in a placeholder."""
    placeholder.info(
        "Elapsed: "
        f"{_format_duration(elapsed_s)} | Estimated remaining: "
        f"{_format_duration(remaining_low_s)} - {_format_duration(remaining_high_s)}"
    )


def render_completion_eta(actual_total_s: float, eta: EtaResult) -> None:
    """Render completion summary comparing actual vs predicted range."""
    actual_label = _format_duration(actual_total_s)
    low_label = _format_duration(eta.total_low_s)
    high_label = _format_duration(eta.total_high_s)
    within = eta.total_low_s <= actual_total_s <= eta.total_high_s

    if within:
        st.success(f"Actual duration: {actual_label} (within estimated range: {low_label} - {high_label})")
    else:
        st.warning(f"Actual duration: {actual_label} (estimated range was {low_label} - {high_label})")


def render_last_run_summary(eta_summary: Mapping[str, Any] | None) -> None:
    """Render persisted last-run actual vs estimated summary."""
    summary = eta_summary or {}
    if not summary:
        return

    st.markdown("---")
    st.info(
        "Last run: "
        f"{int(round(float(summary.get('actual_total_s', 0))))}s "
        f"(estimate was {int(round(float(summary.get('pred_low_s', 0))))}s - "
        f"{int(round(float(summary.get('pred_high_s', 0))))}s)"
    )
    st.caption(
        f"{summary.get('estimate_label', 'Estimated time')} | "
        f"Confidence: {str(summary.get('confidence', 'low')).title()} | "
        f"Similar runs: {summary.get('similar_runs', 0)}"
    )
