"""
Runtime telemetry persistence for ETA estimates.

Stores query/probe runtime events in a local SQLite file.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
import os
import random
import sqlite3


_DEFAULT_DB_FILENAME = "query_runtime_telemetry.sqlite3"
_PRUNE_EVERY_N_WRITES = 20
_ALLOWED_STATUS = {"success", "error", "timeout"}


def get_runtime_db_path() -> Path:
    """Return the runtime telemetry SQLite path."""
    override = os.getenv("SAWGRAPH_RUNTIME_DB_PATH", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (Path(__file__).resolve().parents[1] / "data" / _DEFAULT_DB_FILENAME).resolve()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cutoff_iso(days: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")


def _connect() -> sqlite3.Connection:
    db_path = get_runtime_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def initialize_runtime_store() -> None:
    """Create runtime telemetry tables/indexes if missing."""
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS query_runtime_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                analysis_key TEXT NOT NULL,
                step_label TEXT NOT NULL,
                endpoint_key TEXT NOT NULL,
                status TEXT NOT NULL,
                elapsed_ms REAL NOT NULL,
                row_count INTEGER NULL,
                region_level TEXT NULL,
                state_code TEXT NULL,
                naics_prefix2 TEXT NULL,
                include_nondetects INTEGER NULL,
                has_substance_filter INTEGER NULL,
                has_material_filter INTEGER NULL,
                conc_bin TEXT NULL,
                pred_low_ms REAL NULL,
                pred_mid_ms REAL NULL,
                pred_high_ms REAL NULL,
                pred_confidence TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS probe_runtime_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                endpoint_key TEXT NOT NULL,
                elapsed_ms REAL NOT NULL,
                status TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_qre_analysis_step_time
                ON query_runtime_events (analysis_key, step_label, created_at_utc);

            CREATE INDEX IF NOT EXISTS idx_qre_bucket
                ON query_runtime_events (
                    analysis_key, step_label, region_level, state_code,
                    include_nondetects, conc_bin
                );

            CREATE INDEX IF NOT EXISTS idx_qre_endpoint_time
                ON query_runtime_events (endpoint_key, created_at_utc);

            CREATE INDEX IF NOT EXISTS idx_probe_endpoint_time
                ON probe_runtime_events (endpoint_key, created_at_utc);
            """
        )


def _sanitize_status(status: Any) -> str:
    value = str(status or "").strip().lower()
    if value in _ALLOWED_STATUS:
        return value
    if "timeout" in value:
        return "timeout"
    return "error"


def _normalize_bool(value: Any) -> int | None:
    if value is None:
        return None
    return int(bool(value))


def insert_query_runtime_event(event: Mapping[str, Any], retention_days: int = 90) -> None:
    """Insert one query runtime telemetry row."""
    initialize_runtime_store()

    payload = {
        "created_at_utc": str(event.get("created_at_utc") or _utc_now_iso()),
        "analysis_key": str(event.get("analysis_key") or ""),
        "step_label": str(event.get("step_label") or ""),
        "endpoint_key": str(event.get("endpoint_key") or "federation"),
        "status": _sanitize_status(event.get("status")),
        "elapsed_ms": float(event.get("elapsed_ms") or 0.0),
        "row_count": event.get("row_count"),
        "region_level": event.get("region_level"),
        "state_code": event.get("state_code"),
        "naics_prefix2": event.get("naics_prefix2"),
        "include_nondetects": _normalize_bool(event.get("include_nondetects")),
        "has_substance_filter": _normalize_bool(event.get("has_substance_filter")),
        "has_material_filter": _normalize_bool(event.get("has_material_filter")),
        "conc_bin": event.get("conc_bin"),
        "pred_low_ms": event.get("pred_low_ms"),
        "pred_mid_ms": event.get("pred_mid_ms"),
        "pred_high_ms": event.get("pred_high_ms"),
        "pred_confidence": event.get("pred_confidence"),
    }

    columns = list(payload.keys())
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO query_runtime_events ({', '.join(columns)}) VALUES ({placeholders})"

    with _connect() as conn:
        conn.execute(sql, [payload[c] for c in columns])

    if random.randint(1, _PRUNE_EVERY_N_WRITES) == 1:
        prune_old_events(retention_days=retention_days)


def insert_probe_runtime_event(event: Mapping[str, Any], retention_days: int = 90) -> None:
    """Insert one probe runtime telemetry row."""
    initialize_runtime_store()

    payload = {
        "created_at_utc": str(event.get("created_at_utc") or _utc_now_iso()),
        "endpoint_key": str(event.get("endpoint_key") or "federation"),
        "elapsed_ms": float(event.get("elapsed_ms") or 0.0),
        "status": _sanitize_status(event.get("status")),
    }
    columns = list(payload.keys())
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO probe_runtime_events ({', '.join(columns)}) VALUES ({placeholders})"

    with _connect() as conn:
        conn.execute(sql, [payload[c] for c in columns])

    if random.randint(1, _PRUNE_EVERY_N_WRITES) == 1:
        prune_old_events(retention_days=retention_days)


def prune_old_events(retention_days: int = 90) -> None:
    """Delete rows older than retention window."""
    initialize_runtime_store()
    cutoff = _cutoff_iso(retention_days)
    with _connect() as conn:
        conn.execute("DELETE FROM query_runtime_events WHERE created_at_utc < ?", (cutoff,))
        conn.execute("DELETE FROM probe_runtime_events WHERE created_at_utc < ?", (cutoff,))


def _status_clause(statuses: Sequence[str]) -> tuple[str, list[str]]:
    normalized = [_sanitize_status(s) for s in statuses] or ["success"]
    placeholders = ", ".join(["?"] * len(normalized))
    return f"({placeholders})", normalized


def fetch_query_elapsed_ms(
    analysis_key: str,
    step_label: str,
    endpoint_key: str = "federation",
    filters: Mapping[str, Any] | None = None,
    statuses: Sequence[str] = ("success",),
    days: int = 90,
) -> list[float]:
    """Return elapsed ms values for matching query runtime events."""
    initialize_runtime_store()
    cutoff = _cutoff_iso(days)
    status_in, status_values = _status_clause(statuses)

    sql = (
        "SELECT elapsed_ms FROM query_runtime_events "
        "WHERE analysis_key = ? AND step_label = ? AND endpoint_key = ? "
        f"AND status IN {status_in} "
        "AND created_at_utc >= ?"
    )
    params: list[Any] = [analysis_key, step_label, endpoint_key, *status_values, cutoff]

    for key, value in (filters or {}).items():
        if value is None:
            sql += f" AND {key} IS NULL"
        else:
            sql += f" AND {key} = ?"
            params.append(value)

    sql += " ORDER BY created_at_utc ASC"

    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [float(r["elapsed_ms"]) for r in rows if r["elapsed_ms"] is not None]


def fetch_probe_elapsed_ms(
    endpoint_key: str = "federation",
    statuses: Sequence[str] = ("success",),
    days: int = 30,
) -> list[float]:
    """Return probe elapsed ms values for endpoint/status window."""
    initialize_runtime_store()
    cutoff = _cutoff_iso(days)
    status_in, status_values = _status_clause(statuses)
    sql = (
        "SELECT elapsed_ms FROM probe_runtime_events "
        "WHERE endpoint_key = ? "
        f"AND status IN {status_in} "
        "AND created_at_utc >= ? "
        "ORDER BY created_at_utc ASC"
    )
    params: list[Any] = [endpoint_key, *status_values, cutoff]
    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [float(r["elapsed_ms"]) for r in rows if r["elapsed_ms"] is not None]


def fetch_prediction_errors_percent(
    analysis_key: str,
    endpoint_key: str = "federation",
    days: int = 30,
) -> list[float]:
    """
    Return absolute percentage error values for recent predicted-vs-actual events.

    APE = abs(actual - predicted_mid) / predicted_mid * 100
    """
    initialize_runtime_store()
    cutoff = _cutoff_iso(days)
    sql = (
        "SELECT elapsed_ms, pred_mid_ms FROM query_runtime_events "
        "WHERE analysis_key = ? AND endpoint_key = ? "
        "AND status = 'success' AND pred_mid_ms IS NOT NULL AND pred_mid_ms > 0 "
        "AND created_at_utc >= ?"
    )
    with _connect() as conn:
        rows = conn.execute(sql, (analysis_key, endpoint_key, cutoff)).fetchall()

    errors: list[float] = []
    for row in rows:
        actual = float(row["elapsed_ms"])
        pred = float(row["pred_mid_ms"])
        errors.append(abs(actual - pred) / pred * 100.0)
    return errors
