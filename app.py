"""
SAWGraph PFAS Explorer - Main Application Entry Point
"""
from __future__ import annotations

import os
import streamlit as st

from analysis_registry import AnalysisContext, RegionConfig, build_registry
from core.sparql import ENDPOINT_URLS
from core.data_loader import (
    load_fips_data,
    load_material_types_data,
    load_substances_data,
    parse_regions,
)
from filters.region import (
    RegionSelection,
    render_region_selector,
)
from analyses.sockg_sites.queries import get_sockg_state_code_set
from analyses.aquifer_wells.queries import get_aquifer_state_code_set
from components.start_page import render_start_page


# Project directory
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


def _set_page_config() -> None:
    st.set_page_config(
        page_title="SAWGraph PFAS Explorer",
        page_icon="assets/Sawgraph-Logo-transparent.png",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def main() -> None:
    _set_page_config()

    # Load shared data once (cached)
    fips_df = load_fips_data()
    states_df, counties_df, subdivisions_df = parse_regions(fips_df)
    substances_df = load_substances_data()
    material_types_df = load_material_types_data()

    registry = build_registry()
    enabled_specs = [s for s in registry.values() if s.enabled]
    enabled_specs.sort(key=lambda s: s.label)

    st.sidebar.markdown("### 📊 Select Analysis Type")
    if "analysis_selector_modular" not in st.session_state:
        st.session_state.analysis_selector_modular = "-- Home --"

    home_spacer, home_col = st.sidebar.columns([5, 1])
    with home_col:
        if st.button("🏠", help="Return to the homepage", key="home_btn_modular"):
            st.session_state.analysis_selector_modular = "-- Home --"
            st.rerun()

    label_to_key = {s.label: s.key for s in enabled_specs}
    analysis_label = st.sidebar.selectbox(
        "Choose analysis:",
        ["-- Home --"] + [s.label for s in enabled_specs],
        key="analysis_selector_modular",
    )

    st.sidebar.markdown("---")

    selected_key = label_to_key.get(analysis_label)
    
    # Get region config from the analysis spec, or use a default
    if selected_key:
        spec = registry[selected_key]
        region_config = spec.region_config or RegionConfig()
    else:
        region_config = RegionConfig()
    
    # Determine the availability function based on source
    if region_config.availability_source == "sockg":
        availability_fn = get_sockg_state_code_set
    elif region_config.availability_source == "aquifer":
        availability_fn = get_aquifer_state_code_set
    else:
        availability_fn = None

    # Render the unified region selector using the analysis's config
    region = render_region_selector(
        config=region_config,
        states_df=states_df,
        counties_df=counties_df,
        subdivisions_df=subdivisions_df,
        get_sockg_state_codes_fn=availability_fn,
    )

    if analysis_label == "-- Home --" or not selected_key:
        render_start_page(PROJECT_DIR)
        return

    spec = registry[selected_key]
    context = AnalysisContext(
        states_df=states_df,
        counties_df=counties_df,
        subdivisions_df=subdivisions_df,
        substances_df=substances_df,
        material_types_df=material_types_df,
        selected_state_code=region.state_code,
        selected_state_name=region.state_name,
        selected_county_code=region.county_code,
        selected_county_name=region.county_name,
        selected_subdivision_code=region.subdivision_code,
        selected_subdivision_name=region.subdivision_name,
        region_code=region.region_code,
        region_display=region.region_display,
        endpoints=ENDPOINT_URLS,
        project_dir=PROJECT_DIR,
        analysis_key=spec.key,
        query_number=spec.query,
    )

    st.markdown(f"## {spec.title}")
    st.caption(spec.description)
    spec.runner(context)


if __name__ == "__main__":
    main()
