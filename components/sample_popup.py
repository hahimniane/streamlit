
"""
Sample Popup Utilities

Shared HTML popup builder for sample points on maps. Aggregates raw
per-observation rows into rich HTML popups showing max result, nested
sample sections, and observation tables.
"""
from __future__ import annotations

import html as html_mod
from typing import Dict, List, Optional

import pandas as pd


GROUP_COLS = ["samplePoint", "spWKT", "samplePointName"]

SAMPLE_POPUP_FIELDS = ["samplePointName", "Max Result", "Samples"]

# Lightweight popup fields — used when dataset is too large for full HTML popups
SAMPLE_POPUP_FIELDS_LITE = ["samplePointName", "Max Substance", "Max Result (ng/L)", "Observations"]

SAMPLE_POPUP_KWDS = {"max_width": 900, "max_height": 500, "parse_html": True}


def _is_empty(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    if isinstance(x, str) and (x.strip() == "" or x.lower() == "nan"):
        return True
    return False


def _group_to_html(group: pd.DataFrame) -> pd.Series:
    """Build rich HTML popup content for a single sample point's observations."""
    html_parts: list[str] = []
    max_result_parts: list[str] = []
    overall_max_result = -1.0
    overall_max_unit = None
    overall_max_substance = None
    overall_max_sample_id = None
    overall_max_date = None

    for _, obs_row in group.iterrows():
        result_str = obs_row.get("result")
        if not _is_empty(result_str) and result_str != "non-detect":
            try:
                current_numeric_result = float(result_str)
                if current_numeric_result > overall_max_result:
                    overall_max_result = current_numeric_result
                    overall_max_unit = obs_row.get("unit")
                    overall_max_substance = obs_row.get("substance")
                    overall_max_sample_id = obs_row.get("sampleIdentifier")
                    overall_max_date = obs_row.get("date")
            except ValueError:
                pass

    if overall_max_result != -1.0:
        max_result_parts.append("<div style='padding-bottom: 5px;'>")
        max_result_parts.append(
            f"<b>{html_mod.escape(str(overall_max_substance))}</b>: "
            f"{overall_max_result} {html_mod.escape(str(overall_max_unit))}"
        )
        if not _is_empty(overall_max_sample_id):
            max_result_parts.append(
                f" (from Sample ID: {html_mod.escape(str(overall_max_sample_id))} "
                f"{str(overall_max_date)[0:4]})"
            )
        max_result_parts.append("<br/></div>")

    sample_level_group_cols = ["sample", "sampleIdentifier", "date", "sampleType"]
    available_sample_cols = [c for c in sample_level_group_cols if c in group.columns]

    if available_sample_cols:
        grouped = group.groupby(available_sample_cols, dropna=False, sort=False)
    else:
        grouped = [(None, group)]

    for sample_key_tuple, sample_group_df in grouped:
        if available_sample_cols and sample_key_tuple is not None:
            if len(available_sample_cols) == 4:
                sample_uri_link, sample_id, sample_date, sample_type = sample_key_tuple
            else:
                keys = dict(zip(available_sample_cols, sample_key_tuple if isinstance(sample_key_tuple, tuple) else (sample_key_tuple,)))
                sample_uri_link = keys.get("sample")
                sample_id = keys.get("sampleIdentifier")
                sample_date = keys.get("date")
                sample_type = keys.get("sampleType")
        else:
            sample_uri_link = sample_id = sample_date = sample_type = None

        html_parts.append(
            "<div style='margin-left: 15px; border-bottom: 1px solid #eee; "
            "padding-top: 5px; padding-bottom: 5px;'>"
        )
        if not _is_empty(sample_uri_link):
            uri_str = str(sample_uri_link)
            short = uri_str.split("#")[-1] if "#" in uri_str else uri_str.rsplit("/", 1)[-1]
            html_parts.append(
                f"<b>Sample URI</b>: {html_mod.escape(short)}<br/>"
            )
        if not _is_empty(sample_id):
            html_parts.append(f"<b>Sample ID</b>: {html_mod.escape(str(sample_id))}<br/>")
        if not _is_empty(sample_date):
            html_parts.append(f"<b>Date</b>: {html_mod.escape(str(sample_date))}<br/>")
        if not _is_empty(sample_type):
            html_parts.append(f"<b>Sample Type</b>: {html_mod.escape(str(sample_type))}<br/>")

        html_parts.append(
            "<table style='width:100%; border-collapse: collapse;'>"
            "<thead><tr>"
            "<th style='border: 1px solid #ddd; padding: 2px; text-align: left;'>Substance</th>"
            "<th style='border: 1px solid #ddd; padding: 2px; text-align: left;'>Result</th>"
            "</tr></thead><tbody>"
        )
        for _, obs_row in sample_group_df.iterrows():
            result_val = obs_row.get("result")
            substance_val = obs_row.get("substance")
            unit_val = obs_row.get("unit")
            if not _is_empty(result_val):
                html_parts.append(
                    f"<tr>"
                    f"<td style='border: 1px solid #ddd; padding: 2px;'>{html_mod.escape(str(substance_val))}</td>"
                    f"<td style='border: 1px solid #ddd; padding: 2px;'>{result_val} {html_mod.escape(str(unit_val))}</td>"
                    f"</tr>"
                )
        html_parts.append("</tbody></table></div>")

    return pd.Series({
        "Max Result": "".join(max_result_parts),
        "Samples": "".join(html_parts),
        "overall_max_result": overall_max_result if overall_max_result != -1.0 else None,
    })


def aggregate_sample_popups(
    df: pd.DataFrame,
    group_cols: Optional[List[str]] = None,
    column_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Aggregate raw per-observation rows into popup-ready rows grouped by sample point.

    Parameters
    ----------
    df : pd.DataFrame
        Raw observation rows. Expected canonical columns (after renaming):
        samplePoint, samplePointName, spWKT, sample, sampleIdentifier,
        date, substance, result, unit, sampleType.
    group_cols : list[str], optional
        Columns to group by. Defaults to GROUP_COLS.
    column_map : dict[str, str], optional
        Rename mapping applied before grouping, e.g. {"sp": "samplePoint"}.

    Returns
    -------
    pd.DataFrame
        One row per sample point with columns: samplePoint, samplePointName,
        spWKT, Max Result (HTML), Samples (HTML), overall_max_result (numeric).
    """
    if df.empty:
        return df

    work = df.copy()
    if column_map:
        work = work.rename(columns=column_map)

    grp = group_cols or GROUP_COLS
    available_grp = [c for c in grp if c in work.columns]
    if not available_grp:
        return work

    agg = (
        work.groupby(available_grp, dropna=False, sort=False)
        .apply(_group_to_html, include_groups=False)
        .reset_index()
    )
    return agg


def _group_to_lite(group: pd.DataFrame) -> pd.Series:
    """Lightweight aggregation: max result + count only, no HTML."""
    max_val = -1.0
    max_substance = None
    for _, row in group.iterrows():
        r = row.get("result")
        if _is_empty(r) or r == "non-detect":
            continue
        try:
            v = float(r)
            if v > max_val:
                max_val = v
                max_substance = row.get("substance")
        except ValueError:
            pass
    return pd.Series({
        "Max Substance": max_substance if max_val != -1.0 else None,
        "Max Result (ng/L)": round(max_val, 2) if max_val != -1.0 else None,
        "Observations": len(group),
        "overall_max_result": max_val if max_val != -1.0 else None,
    })


def aggregate_sample_popups_lite(
    df: pd.DataFrame,
    group_cols: Optional[List[str]] = None,
    column_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Lightweight aggregation — plain text summary, no HTML.

    Use this instead of aggregate_sample_popups when the dataset is large
    to avoid embedding megabytes of HTML in the map GeoJSON.
    """
    if df.empty:
        return df
    work = df.copy()
    if column_map:
        work = work.rename(columns=column_map)
    grp = group_cols or GROUP_COLS
    available_grp = [c for c in grp if c in work.columns]
    if not available_grp:
        return work
    return (
        work.groupby(available_grp, dropna=False, sort=False)
        .apply(_group_to_lite, include_groups=False)
        .reset_index()
    )
