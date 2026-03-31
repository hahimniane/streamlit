"""
Concentration Utilities
- UI components for concentration range filtering
- SPARQL queries to get concentration bounds based on region/substance/material filters
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import streamlit as st

from core.sparql import ENDPOINT_URLS, execute_sparql_query


@dataclass
class ConcentrationFilterResult:
    """Result from the concentration filter UI."""
    min_concentration: int
    max_concentration: int
    include_nondetects: bool


def render_concentration_filter(
    analysis_key: str,
    default_min: int = 0,
    default_max: int = 100,
    slider_max: int = 500,
    show_header: bool = True,
) -> ConcentrationFilterResult:
    """
    Render concentration filter UI in the sidebar.
    
    Includes:
    - Min/max number inputs
    - Slider for quick selection (0-500 range)
    - Include nondetects checkbox
    
    Args:
        analysis_key: Unique key prefix for session state
        default_min: Default minimum concentration
        default_max: Default maximum concentration
        slider_max: Maximum value for the slider
        show_header: Whether to show the "📊 Detected Concentration" header
    
    Returns:
        ConcentrationFilterResult with min, max, and include_nondetects values
    """
    # Session state keys
    conc_min_key = f"{analysis_key}_conc_min"
    conc_max_key = f"{analysis_key}_conc_max"
    include_nondetects_key = f"{analysis_key}_include_nondetects"
    include_nondetects_pending_key = f"{analysis_key}_include_nondetects_pending"
    min_pending_key = f"{analysis_key}_conc_min_pending"
    max_pending_key = f"{analysis_key}_conc_max_pending"
    slider_key = f"{analysis_key}_concentration_slider"
    
    # Initialize session state
    if conc_min_key not in st.session_state:
        st.session_state[conc_min_key] = default_min
    if conc_max_key not in st.session_state:
        st.session_state[conc_max_key] = default_max
    if include_nondetects_key not in st.session_state:
        st.session_state[include_nondetects_key] = False
    if include_nondetects_pending_key not in st.session_state:
        st.session_state[include_nondetects_pending_key] = st.session_state[include_nondetects_key]
    
    # Header
    if show_header:
        st.sidebar.markdown("### 📊 Detected Concentration")
    
    # Include nondetects checkbox
    include_nondetects = st.sidebar.checkbox(
        "Include nondetects",
        value=st.session_state[include_nondetects_pending_key],
        key=f"{analysis_key}_nondetects_checkbox_pending",
        help="Include observations flagged as non-detect (included alongside detected results in range)",
    )
    st.session_state[include_nondetects_pending_key] = include_nondetects
    
    # Normalize applied values
    applied_min = max(0, int(st.session_state[conc_min_key]))
    applied_max = max(0, int(st.session_state[conc_max_key]))
    if applied_min > applied_max:
        applied_max = applied_min
    st.session_state[conc_min_key] = applied_min
    st.session_state[conc_max_key] = applied_max
    
    # Initialize pending keys
    if min_pending_key not in st.session_state:
        st.session_state[min_pending_key] = applied_min
    if max_pending_key not in st.session_state:
        st.session_state[max_pending_key] = applied_max
    if slider_key not in st.session_state:
        st.session_state[slider_key] = (
            min(st.session_state[min_pending_key], slider_max),
            min(st.session_state[max_pending_key], slider_max),
        )
    
    # Normalize pending values
    st.session_state[min_pending_key] = max(0, int(st.session_state[min_pending_key]))
    st.session_state[max_pending_key] = max(0, int(st.session_state[max_pending_key]))
    if st.session_state[min_pending_key] > st.session_state[max_pending_key]:
        st.session_state[max_pending_key] = st.session_state[min_pending_key]
    
    # Callback functions for syncing slider <-> inputs
    def _on_slider_change() -> None:
        smn, smx = st.session_state.get(slider_key, (0, 0))
        st.session_state[min_pending_key] = int(smn)
        st.session_state[max_pending_key] = int(smx)
    
    def _on_minmax_change() -> None:
        mn = max(0, int(st.session_state.get(min_pending_key, 0)))
        mx = max(0, int(st.session_state.get(max_pending_key, 0)))
        if mn > mx:
            mx = mn
        st.session_state[min_pending_key] = mn
        st.session_state[max_pending_key] = mx
        if mn <= slider_max and mx <= slider_max:
            st.session_state[slider_key] = (mn, mx)
    
    # Min/Max number inputs
    min_col, max_col = st.sidebar.columns(2)
    min_col.number_input(
        "Min (ng/L)",
        min_value=0,
        step=1,
        format="%d",
        key=min_pending_key,
        on_change=_on_minmax_change,
    )
    max_col.number_input(
        "Max (ng/L)",
        min_value=0,
        step=1,
        format="%d",
        key=max_pending_key,
        on_change=_on_minmax_change,
    )
    
    # Slider
    st.sidebar.slider(
        "Select concentration range (ng/L)",
        min_value=0,
        max_value=slider_max,
        value=(
            int(min(st.session_state[min_pending_key], slider_max)),
            int(min(st.session_state[max_pending_key], slider_max)),
        ),
        step=1,
        key=slider_key,
        help="Drag to select min and max concentration in nanograms per liter",
        on_change=_on_slider_change,
    )
    
    # Calculate final values
    min_concentration = max(0, int(st.session_state[min_pending_key]))
    max_concentration = max(0, int(st.session_state[max_pending_key]))
    if min_concentration > max_concentration:
        max_concentration = min_concentration
    
    # Display selected range
    st.sidebar.markdown(f"**Selected range:** {min_concentration} - {max_concentration} ng/L")
    st.sidebar.markdown("---")
    
    return ConcentrationFilterResult(
        min_concentration=min_concentration,
        max_concentration=max_concentration,
        include_nondetects=include_nondetects,
    )


def apply_concentration_filter(analysis_key: str) -> Tuple[int, int, bool]:
    """
    Apply pending concentration filter values to session state.
    Call this when the Execute button is clicked.
    
    Returns:
        Tuple of (min_concentration, max_concentration, include_nondetects)
    """
    conc_min_key = f"{analysis_key}_conc_min"
    conc_max_key = f"{analysis_key}_conc_max"
    include_nondetects_key = f"{analysis_key}_include_nondetects"
    include_nondetects_pending_key = f"{analysis_key}_include_nondetects_pending"
    min_pending_key = f"{analysis_key}_conc_min_pending"
    max_pending_key = f"{analysis_key}_conc_max_pending"
    
    # Apply pending values
    st.session_state[include_nondetects_key] = st.session_state.get(include_nondetects_pending_key, False)
    
    min_concentration = max(0, int(st.session_state.get(min_pending_key, 0)))
    max_concentration = max(0, int(st.session_state.get(max_pending_key, 0)))
    if min_concentration > max_concentration:
        max_concentration = min_concentration
    
    st.session_state[conc_min_key] = min_concentration
    st.session_state[conc_max_key] = max_concentration
    
    return min_concentration, max_concentration, st.session_state[include_nondetects_key]


def _parse_max_value(results: dict) -> Optional[float]:
    if not results or "results" not in results:
        return None
    bindings = results["results"].get("bindings", [])
    if not bindings:
        return None
    max_val = None
    for binding in bindings:
        if isinstance(binding, dict) and "max" in binding:
            max_val = binding.get("max", {}).get("value")
            break
    try:
        return float(max_val) if max_val is not None else None
    except (TypeError, ValueError):
        return None


def get_max_concentration(
    region_code: str,
    is_subdivision: bool = False,
    substance_uri: Optional[str] = None,
    material_uri: Optional[str] = None,
) -> Optional[float]:
    """
    Get the maximum concentration (ng/L) for the selected region and filters.

    Args:
        region_code: FIPS code for county or subdivision geoId
        is_subdivision: True if region_code is a subdivision geoId
        substance_uri: Optional substance URI to filter
        material_uri: Optional material type URI to filter

    Returns:
        Maximum concentration value (float) or None if unavailable
    """
    if not region_code:
        return None

    if is_subdivision:
        region_pattern = (
            f"?sp rdf:type coso:SamplePoint ;"
            f" kwg-ont:sfWithin|kwg-ont:sfTouches <https://datacommons.org/browser/geoId/{region_code}> ."
        )
    else:
        region_pattern = (
            f"?sp rdf:type coso:SamplePoint ;"
            f" kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .\n"
            f"?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;"
            f" kwg-ont:administrativePartOf <http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{region_code}> ."
        )

    substance_filter = f"VALUES ?substance {{<{substance_uri}>}}" if substance_uri else ""
    material_filter = f"VALUES ?matType {{<{material_uri}>}}" if material_uri else ""

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT (MAX(?numericValue) as ?max) WHERE {{
    {region_pattern}
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofDSSToxSubstance ?substance ;
                coso:analyzedSample ?sample ;
                coso:hasResult ?result .
    ?sample coso:sampleOfMaterialType ?matType .
    ?result coso:measurementValue ?result_value ;
            coso:measurementUnit ?unit .
    OPTIONAL {{ ?result qudt:quantityValue/qudt:numericValue ?numericResult }}
    OPTIONAL {{ ?result qudt:enumeratedValue ?enumDetected }}
    FILTER(!BOUND(?enumDetected))
    BIND(COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value)) as ?numericValue)
    VALUES ?unit {{<http://qudt.org/vocab/unit/NanoGM-PER-L>}}
    {substance_filter}
    {material_filter}
}}
"""

    results = execute_sparql_query(
        ENDPOINT_URLS["federation"], query, timeout=300,
        label=f"Filter: Max Concentration (region {region_code})",
    )
    return _parse_max_value(results)
