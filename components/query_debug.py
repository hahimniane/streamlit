"""
Shared debug renderer for executed SPARQL queries.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping
import streamlit as st


def _build_metadata_line(query_info: Mapping[str, Any]) -> str:
    """Build a compact metadata line for one executed query."""
    parts: list[str] = []

    endpoint = query_info.get("endpoint")
    if endpoint:
        parts.append(f"Endpoint: `{endpoint}`")

    timeout = query_info.get("timeout_sec")
    if timeout is not None:
        parts.append(f"Timeout: `{timeout}s`")

    response_status = query_info.get("response_status")
    if response_status is not None:
        parts.append(f"Status: `{response_status}`")

    elapsed_ms = query_info.get("elapsed_ms")
    if elapsed_ms is not None:
        try:
            parts.append(f"Elapsed: `{float(elapsed_ms) / 1000.0:.2f}s`")
        except (TypeError, ValueError):
            pass

    row_count = query_info.get("row_count")
    if row_count is not None:
        parts.append(f"Rows: `{row_count}`")

    return " | ".join(parts)


def render_executed_queries(
    executed_queries: Iterable[Mapping[str, Any]] | None,
    title: str = "Debug: Executed Queries",
) -> None:
    """
    Render executed SPARQL queries with copyable code blocks.

    Args:
        executed_queries: Iterable of query metadata dicts. Each item may include
            label, endpoint, timeout_sec, response_status, row_count, error, query.
        title: Expander title.
    """
    queries = list(executed_queries or [])
    if not queries:
        return

    with st.expander(title):
        st.caption("Exact query text sent to the endpoint. Use the copy button in each code block.")

        for index, query_info in enumerate(queries, start=1):
            label = query_info.get("label") or f"Query {index}"
            st.markdown(f"**{label}**")

            metadata = _build_metadata_line(query_info)
            if metadata:
                st.caption(metadata)

            query_text = str(query_info.get("query") or "").strip()
            if query_text:
                st.code(query_text, language="sparql")
            else:
                st.info("No query text captured for this step.")

            error_message = query_info.get("error") or query_info.get("exception")
            if error_message:
                st.error(f"Error: {error_message}")

            if index < len(queries):
                st.markdown("---")
