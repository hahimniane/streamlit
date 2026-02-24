"""
Samples Near Facilities Analysis (Query 2)
Find contaminated samples near facilities of a specific industry type
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
from streamlit_folium import st_folium

from analysis_registry import AnalysisContext
from analyses.samples_near_facilities.queries import execute_nearby_analysis
from core.data_loader import load_naics_dict
from filters.industry import render_hierarchical_naics_selector
from filters.concentration import render_concentration_filter, apply_concentration_filter

# Shared components
from core.boundary import fetch_boundaries
from core.geometry import create_geodataframe
from components.parameter_display import render_parameter_table
from components.result_display import render_metrics_row, render_data_expander, clean_unit_encoding
from components.map_rendering import create_base_map, add_boundary_layers, add_point_layer, finalize_map, render_map_legend
from components.execute_button import render_execute_button
from components.analysis_state import AnalysisState, check_old_session_keys
from components.step_execution import StepExecutor
from components.query_debug import render_executed_queries


def main(context: AnalysisContext) -> None:
    """Main function for Samples Near Facilities analysis"""
    check_old_session_keys(['q2_conc_min', 'q2_conc_max', 'q2_executed', 'q2_facilities', 'q2_samples'])

    st.markdown("""
    **What this analysis does:**
    - Find all facilities of a specific industry type (optionally filtered by region)
    - Expand search to neighboring areas
    - Identify PFAS samples near those facilities

    **3-Step Process:** Find facilities -> Expand to neighboring areas -> Identify PFAS samples

    **Use case:** Determine if PFAS contamination exists near specific industries
    """)

    state = AnalysisState(context.analysis_key)
    state.init_if_missing("executed_queries", [])

    # === SIDEBAR PARAMETERS ===
    naics_dict = load_naics_dict()
    st.sidebar.markdown("### Industry Type")
    st.sidebar.markdown("_Optional: leave empty to search all industries_")
    selected_naics_code = render_hierarchical_naics_selector(
        naics_dict=naics_dict,
        key=f"{context.analysis_key}_industry_selector",
        default_value=None,
        allow_empty=True,
    )

    selected_industry_display = (
        f"{selected_naics_code} - {naics_dict.get(selected_naics_code, 'Unknown')}"
        if selected_naics_code else "All Industries"
    )

    conc_filter = render_concentration_filter(context.analysis_key)

    execute_clicked = render_execute_button(help_text="Execute the nearby facilities analysis")

    # === QUERY EXECUTION ===
    if execute_clicked:
        min_conc, max_conc, include_nondetects = apply_concentration_filter(context.analysis_key)

        st.markdown("---")
        st.subheader("Query Execution")

        executor = StepExecutor(num_steps=3)
        facilities_df = pd.DataFrame()
        samples_df = pd.DataFrame()
        executed_queries = []

        with executor.step(1, "Finding facilities...") as step:
            facilities_df, samples_df, debug_info = execute_nearby_analysis(
                naics_code=selected_naics_code, region_code=context.region_code,
                min_concentration=min_conc, max_concentration=max_conc,
                include_nondetects=include_nondetects)
            executed_queries = list((debug_info or {}).get("queries", []))
            if not facilities_df.empty:
                step.success(f"Step 1: Found {len(facilities_df)} facilities")
            else:
                step.warning("Step 1: No facilities found")

        with executor.step(2, "Expanding to neighboring areas...") as step:
            step.success("Step 2: Expanded to neighboring areas")

        with executor.step(3, "Finding PFAS samples...") as step:
            if not samples_df.empty:
                step.success(f"Step 3: Found {len(samples_df)} PFAS samples")
            else:
                step.info("Step 3: No PFAS samples found")

        boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)

        state.set("executed_queries", executed_queries)
        state.set_results({
            "facilities_df": facilities_df, "samples_df": samples_df,
            "industry_display": selected_industry_display, "boundaries": boundaries,
            "params_data": [
                {"Parameter": "Industry Type", "Value": selected_industry_display},
                {"Parameter": "Geographic Region", "Value": context.region_display or "All Regions"},
                {"Parameter": "Detected Concentration", "Value": f"{min_conc} - {max_conc} ng/L"},
                {"Parameter": "Include nondetects", "Value": "Yes" if include_nondetects else "No"},
            ],
            "query_region_code": context.region_code,
            "executed_queries": executed_queries,
        })

    render_executed_queries(state.get("executed_queries", []))

    # === DISPLAY RESULTS ===
    if state.has_results:
        results = state.get_results()
        facilities_df = results.get("facilities_df", pd.DataFrame())
        samples_df = results.get("samples_df", pd.DataFrame())
        industry_display = results.get("industry_display", "")
        boundaries = results.get("boundaries", {})
        params_data = results.get("params_data", [])
        query_region_code = results.get("query_region_code")

        st.markdown("---")
        render_parameter_table(params_data)
        st.markdown("### Query Results")
        st.markdown("---")

        # Step 1: Facilities
        if not facilities_df.empty:
            st.markdown("### Step 1: Facilities")
            metrics = [{"label": "Total Facilities", "value": len(facilities_df)}]
            if 'industryName' in facilities_df.columns:
                metrics.append({"label": "Industry Types", "value": facilities_df['industryName'].nunique()})
            render_metrics_row(metrics, num_columns=2)
            render_data_expander("View Facilities Data", facilities_df,
                display_columns=['facilityName', 'industryCode', 'industryName', 'facility'],
                download_filename=f"near_facilities_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_facilities")

        # Step 2: Samples
        if not samples_df.empty:
            st.markdown("### Step 2: PFAS Samples")
            metrics = [{"label": "Total Samples", "value": len(samples_df)}]
            if 'sp' in samples_df.columns:
                metrics.append({"label": "Unique Sample Points", "value": samples_df['sp'].nunique()})
            if 'max' in samples_df.columns:
                max_vals = pd.to_numeric(samples_df['max'], errors='coerce')
                if max_vals.notna().any():
                    metrics.append({"label": "Max Concentration", "value": f"{max_vals.max():.2f} ng/L"})
            render_metrics_row(metrics, num_columns=3)

            samples_display = clean_unit_encoding(samples_df, columns=['unit', 'datedresults', 'results'])
            render_data_expander("View Samples Data", samples_display,
                display_columns=['max', 'resultCount', 'datedresults', 'Materials', 'Type', 'spName', 'sp'],
                download_filename=f"near_facilities_samples_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_samples",
                show_stats=True, stats_column='max')

        # Map
        _render_map(facilities_df, samples_df, industry_display, boundaries, query_region_code, context)

        if facilities_df.empty and samples_df.empty:
            st.warning("No results found. Try a different industry type or region.")
    else:
        st.info("Select parameters in the sidebar and click 'Execute Query' to run the analysis")


def _render_map(facilities_df, samples_df, industry_display, boundaries, query_region_code, context) -> None:
    """Render the interactive map."""
    if facilities_df.empty or 'facWKT' not in facilities_df.columns:
        if not facilities_df.empty:
            st.warning("No facility location data available for mapping")
        return

    st.markdown("---")
    st.markdown("### Interactive Map")

    try:
        facilities_gdf = create_geodataframe(facilities_df, 'facWKT')
        if facilities_gdf is None or facilities_gdf.empty:
            return

        map_obj = create_base_map(gdf_list=[facilities_gdf], zoom=8)
        add_boundary_layers(map_obj, boundaries, query_region_code)

        # Add facility links and shorten NAICS codes
        if "facility" in facilities_gdf.columns:
            facilities_gdf["facility_link"] = facilities_gdf["facility"].apply(
                lambda x: f'<a href="https://frs-public.epa.gov/ords/frs_public2/fii_query_detail.disp_program_facility?p_registry_id={x.split(".")[-1]}" target="_blank">FRS {x.split(".")[-1]}</a>' if x else x)
        if "industryCode" in facilities_gdf.columns:
            facilities_gdf["industryCode_short"] = facilities_gdf["industryCode"].apply(
                lambda x: str(x).split("#")[-1] if x else x)

        facility_fields = [c for c in ["facility_link", "facilityName", "industryName", "industryCode_short"] if c in facilities_gdf.columns]
        add_point_layer(map_obj, facilities_gdf,
            name=f'<span style="color:Blue;">{industry_display} ({len(facilities_gdf)})</span>',
            color='Blue', popup_fields=facility_fields, radius=8,
            popup_kwds={"max_width": 650, "parse_html": True},
            tooltip_kwds={"sticky": True, "parse_html": True})

        # Add samples
        if not samples_df.empty and 'spWKT' in samples_df.columns:
            samples_gdf = create_geodataframe(samples_df, 'spWKT')
            if samples_gdf is not None and not samples_gdf.empty:
                samples_gdf = clean_unit_encoding(samples_gdf)
                samples_gdf = samples_gdf.drop(columns=[c for c in ["results", "dates"] if c in samples_gdf.columns], errors="ignore")
                sample_fields = [c for c in ["resultCount", "max", "datedresults", "Materials", "Type", "spName"] if c in samples_gdf.columns]
                add_point_layer(map_obj, samples_gdf,
                    name=f'<span style="color:DarkOrange;">PFAS Samples ({len(samples_gdf)})</span>',
                    color='DarkOrange', popup_fields=sample_fields, radius=6,
                    popup_kwds={'max_height': 450, 'max_width': 450})

        finalize_map(map_obj)
        st_folium(map_obj, width=None, height=600, returned_objects=[])
        render_map_legend([
            "**Boundary** = Selected region",
            "**Blue markers** = Facilities of selected industry type",
            "**Orange markers** = PFAS sample points nearby"
        ])

    except Exception as e:
        st.error(f"Error creating map: {e}")
