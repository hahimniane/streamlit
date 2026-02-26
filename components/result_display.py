"""
Shared result display components for analyses.
Consolidates repeated metrics, expanders, and download buttons.
"""
from __future__ import annotations

from typing import List, Dict, Any, Optional, Callable
import streamlit as st
import pandas as pd


def render_metrics_row(metrics: List[Dict[str, Any]], num_columns: Optional[int] = None) -> None:
    """
    Render a row of metrics in columns.

    Args:
        metrics: List of dicts with 'label' and 'value' keys, optionally 'delta'
        num_columns: Number of columns (defaults to len(metrics))

    Example:
        render_metrics_row([
            {"label": "Total Samples", "value": 150},
            {"label": "Max Concentration", "value": "45.2 ng/L"},
        ])
    """
    if not metrics:
        return

    cols = st.columns(num_columns or len(metrics))
    for i, metric in enumerate(metrics):
        with cols[i]:
            st.metric(
                label=metric.get('label', ''),
                value=metric.get('value', ''),
                delta=metric.get('delta')
            )


def render_data_expander(
    title: str,
    df: pd.DataFrame,
    display_columns: Optional[List[str]] = None,
    download_filename: Optional[str] = None,
    download_key: Optional[str] = None,
    show_stats: bool = False,
    stats_column: Optional[str] = None,
    column_config: Optional[Dict] = None,
) -> None:
    """
    Render an expander with a dataframe, optional download button, and optional statistics.

    Args:
        title: Expander title (e.g., "View Facilities Data")
        df: DataFrame to display
        display_columns: List of columns to show (None = show all)
        download_filename: Filename for CSV download (None = no download button)
        download_key: Unique key for the download button
        show_stats: Whether to show concentration statistics
        stats_column: Column name for statistics (e.g., 'max', 'Max')
        column_config: Optional dict of st.column_config overrides for st.dataframe
    """
    if df is None or df.empty:
        return

    with st.expander(title):
        # Filter to display columns if specified
        if display_columns:
            available_cols = [c for c in display_columns if c in df.columns]
            if available_cols:
                st.dataframe(df[available_cols], use_container_width=True, column_config=column_config)
            else:
                st.dataframe(df, use_container_width=True, column_config=column_config)
        else:
            st.dataframe(df, use_container_width=True, column_config=column_config)

        # Show statistics if requested
        if show_stats and stats_column and stats_column in df.columns:
            st.markdown("##### Concentration Statistics")
            try:
                vals = pd.to_numeric(df[stats_column], errors='coerce')
                if vals.notna().any():
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Max (ng/L)", f"{vals.max():.2f}")
                    with col2:
                        st.metric("Mean (ng/L)", f"{vals.mean():.2f}")
                    with col3:
                        st.metric("Median (ng/L)", f"{vals.median():.2f}")
            except Exception:
                pass

        # Download button
        if download_filename and download_key:
            csv_data = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=download_filename,
                mime="text/csv",
                key=download_key
            )


def render_step_results(
    step_title: str,
    df: pd.DataFrame,
    metrics: List[Dict[str, Any]],
    expander_title: str = "View Data",
    display_columns: Optional[List[str]] = None,
    download_filename: Optional[str] = None,
    download_key: Optional[str] = None,
    show_stats: bool = False,
    stats_column: Optional[str] = None,
    column_config: Optional[Dict] = None,
) -> None:
    """
    Render a complete step result section with title, metrics, and data expander.

    Args:
        step_title: Section title (e.g., "Step 1: Facilities")
        df: DataFrame with results
        metrics: List of metric dicts for render_metrics_row
        expander_title: Title for the data expander
        display_columns: Columns to show in the expander
        download_filename: Filename for CSV download
        download_key: Unique key for download button
        show_stats: Whether to show statistics
        stats_column: Column for statistics
        column_config: Optional dict of st.column_config overrides for st.dataframe
    """
    if df is None or df.empty:
        return

    st.markdown(f"### {step_title}")
    render_metrics_row(metrics)
    render_data_expander(
        title=expander_title,
        df=df,
        display_columns=display_columns,
        download_filename=download_filename,
        download_key=download_key,
        show_stats=show_stats,
        stats_column=stats_column,
        column_config=column_config,
    )


def clean_unit_encoding(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """
    Clean up unit encoding issues (e.g., 'Î¼' -> 'μ').

    Args:
        df: DataFrame to clean
        columns: Columns to clean (default: ['unit', 'datedresults', 'results'])

    Returns:
        DataFrame with cleaned encoding
    """
    if df is None or df.empty:
        return df

    result = df.copy()
    target_cols = columns or ['unit', 'datedresults', 'results']

    for col in target_cols:
        if col in result.columns:
            mask = result[col].notna()
            result.loc[mask, col] = (
                result.loc[mask, col].astype(str).str.replace('Î¼', 'μ')
            )

    return result
