"""
Substance Filtering Utilities

Fetches PFAS substances with labels and observation counts from the knowledge
graph via a single SPARQL query.  No external API calls are needed.
"""
from __future__ import annotations

from typing import List, Optional, Tuple
import pandas as pd
import streamlit as st

from core.sparql import ENDPOINT_URLS, parse_sparql_results, execute_sparql_query


def _fallback_substance_name(substance_uri: str) -> str:
    cleaned = substance_uri.rstrip("/")
    if "#" in cleaned:
        return cleaned.rsplit("#", 1)[-1]
    return cleaned.rsplit("/", 1)[-1]


def get_available_substances_with_labels(
    region_code: str,
    is_subdivision: bool = False,
) -> pd.DataFrame:
    """Get substances with labels and observation counts for a region.

    Returns a DataFrame with columns:
        substance     – URI
        label         – full name (dcterms:alternative)
        short_label   – abbreviation (skos:altLabel), may be NaN
        num           – observation count in the region
        display_name  – short_label if available, else label, else URI tail
    """
    if is_subdivision:
        region_pattern = (
            f"?sp rdf:type coso:SamplePoint ;\n"
            f"        kwg-ont:sfWithin|kwg-ont:sfTouches "
            f"<https://datacommons.org/browser/geoId/{region_code}> ."
        )
    else:
        region_pattern = (
            f"?sp rdf:type coso:SamplePoint ;\n"
            f"        spatial:connectedTo ?region .\n"
            f"    ?region rdf:type kwg-ont:AdministrativeRegion_3 ;\n"
            f"         kwg-ont:administrativePartOf+ "
            f"<http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{region_code}> ."
        )

    query = f"""
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX comptox: <http://w3id.org/DSSTox/v1/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>

SELECT ?substance (SAMPLE(?_label) AS ?label) (SAMPLE(?_short_label) AS ?short_label) (COUNT(DISTINCT ?observation) AS ?num) WHERE {{
    {region_pattern}
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofDSSToxSubstance ?substance .
    ?substance rdf:type comptox:ChemicalEntity .
    ?substance dcterms:alternative ?_label .
    OPTIONAL {{ ?substance skos:altLabel ?_short_label . }}
}} GROUP BY ?substance
"""

    results = execute_sparql_query(
        ENDPOINT_URLS["federation"], query,
        label=f"Filter: Available Substances (region {region_code})",
    )
    if not results:
        return pd.DataFrame(columns=["substance", "label", "short_label", "num", "display_name"])

    df = parse_sparql_results(results)
    if df.empty:
        return pd.DataFrame(columns=["substance", "label", "short_label", "num", "display_name"])

    df = df.dropna(subset=["substance"]).copy()

    if "num" in df.columns:
        df["num"] = pd.to_numeric(df["num"], errors="coerce").fillna(0).astype(int)
    else:
        df["num"] = 0

    # Build display_name: prefer short_label, fall back to label, then URI
    def _build_display(row):
        if pd.notna(row.get("short_label")) and str(row["short_label"]).strip():
            return str(row["short_label"]).strip()
        if pd.notna(row.get("label")) and str(row["label"]).strip():
            return str(row["label"]).strip()
        return _fallback_substance_name(row["substance"])

    df["display_name"] = df.apply(_build_display, axis=1)

    # Aggregate: sum counts per substance URI, keep first label
    df = (
        df.groupby("substance", sort=False)
        .agg({"label": "first", "short_label": "first", "num": "sum", "display_name": "first"})
        .reset_index()
    )

    return df[["substance", "label", "short_label", "num", "display_name"]].reset_index(drop=True)


@st.cache_data(ttl=3600)
def get_cached_substances_with_labels(
    region_code: str,
    is_subdivision: bool = False,
) -> pd.DataFrame:
    """Cached wrapper for region-scoped substance availability."""
    return get_available_substances_with_labels(region_code, is_subdivision)


def get_available_substances(region_code: str, is_subdivision: bool = False) -> List[str]:
    """Get list of substance URIs that have observations in the given region."""
    df = get_available_substances_with_labels(region_code, is_subdivision)
    if df.empty:
        return []
    return df["substance"].tolist()


def render_sidebar_substance_selector(
    region_code: Optional[str],
    analysis_key: str,
    heading: str = "### PFAS Substance",
    allow_empty: bool = True,
    empty_label: str = "All Substances",
) -> Tuple[Optional[str], Optional[str]]:
    """Render the substance dropdown in the sidebar.

    Returns (selected_substance_uri, selected_display_name).
    Both are None when the user picks the empty/all option.
    """
    is_subdivision = len(region_code) > 5 if region_code else False
    substances_df = (
        get_cached_substances_with_labels(region_code, is_subdivision)
        if region_code
        else pd.DataFrame()
    )

    st.sidebar.markdown(heading)

    # Build display -> URI mapping with counts in the label
    display_to_uri = {}
    if not substances_df.empty:
        for _, row in substances_df.iterrows():
            name = row["display_name"]
            uri = row["substance"]
            count = int(row.get("num", 0))
            display_label = f"{name} ({count})" if count > 0 else name
            if display_label not in display_to_uri or str(uri).endswith("_A"):
                display_to_uri[display_label] = (uri, name)

    options = sorted(display_to_uri.keys())
    if allow_empty:
        placeholder = f"-- {empty_label} --"
        options = [placeholder] + options

    selected = st.sidebar.selectbox(
        "Select PFAS Substance (Optional)" if allow_empty else "Select PFAS Substance",
        options,
        help="Filter to a specific PFAS compound" + (f", or leave as '{empty_label}'" if allow_empty else ""),
    )

    if allow_empty and selected == placeholder:
        return None, None

    uri, display_name = display_to_uri.get(selected, (None, None))
    return uri, display_name
