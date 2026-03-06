"""
Samples Near Facilities Analysis (Query 2)
Find contaminated samples near facilities of a specific industry type
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from analysis_registry import AnalysisContext
from analyses.samples_near_facilities.queries import (
    execute_nearby_facilities_query,
    execute_nearby_samples_query,
)
from filters.industry import render_sidebar_industry_selector
from filters.concentration import render_concentration_filter, apply_concentration_filter

# Shared components
from core.boundary import fetch_boundaries
from core.geometry import create_geodataframe
from core.sparql import build_query_debug_entry
from components.parameter_display import (
    build_concentration_params,
    build_industry_params,
    build_region_params,
    render_parameter_table,
)
from components.result_display import clean_unit_encoding, render_step_results
from components.map_rendering import (
    FACILITY_MARKER_RADIUS,
    add_facility_link_column,
    add_naics_link_column,
    add_naics_url_column,
    create_base_map,
    add_boundary_layers,
    add_point_layer,
    finalize_map,
    render_map_legend,
    render_folium_map,
)
from components.execute_button import render_execute_button
from components.analysis_state import AnalysisState, check_old_session_keys
from components.step_execution import StepExecutor
from components.query_debug import render_executed_queries
from components.eta_display import (
    render_simple_eta,
)
from core.runtime_eta import (
    build_eta_request,
    estimate_eta,
    naics_prefix2_from_code,
    record_executed_query_batch,
)


def main(context: AnalysisContext) -> None:
    """Main function for Samples Near Facilities analysis"""
    check_old_session_keys(['q2_conc_min', 'q2_conc_max', 'q2_executed', 'q2_facilities', 'q2_samples'])

    st.markdown("""
    **What this analysis does:**
    - Find all facilities of a specific industry type (optionally filtered by region)
    - Identify PFAS samples near those facilities

    **2-Step Process:** Find facilities -> Identify PFAS samples nearby

    **Use case:** Determine if PFAS contamination exists near specific industries
    """)

    state = AnalysisState(context.analysis_key)
    state.init_if_missing("executed_queries", [])

    # === SIDEBAR PARAMETERS ===
    selected_naics_code, selected_industry_display = render_sidebar_industry_selector(
        analysis_key=context.analysis_key,
        heading="### Industry Type",
        caption="_Optional: leave empty to search all industries_",
        allow_empty=True,
        empty_label="All Industries",
    )

    conc_filter = render_concentration_filter(context.analysis_key)

    execute_clicked = render_execute_button(help_text="Execute the nearby facilities analysis")

    preview_request = build_eta_request(
        analysis_key=context.analysis_key,
        region_code=context.region_code,
        state_code=context.selected_state_code,
        min_conc=conc_filter.min_concentration,
        max_conc=conc_filter.max_concentration,
        include_nondetects=conc_filter.include_nondetects,
        naics_prefix2=naics_prefix2_from_code(selected_naics_code),
        has_substance_filter=False,
        has_material_filter=False,
    )
    render_simple_eta(estimate_eta(preview_request))

    # === QUERY EXECUTION ===
    if execute_clicked:
        min_conc, max_conc, include_nondetects = apply_concentration_filter(context.analysis_key)

        st.markdown("---")
        st.subheader("Query Execution")

        run_request = build_eta_request(
            analysis_key=context.analysis_key,
            region_code=context.region_code,
            state_code=context.selected_state_code,
            min_conc=min_conc,
            max_conc=max_conc,
            include_nondetects=include_nondetects,
            naics_prefix2=naics_prefix2_from_code(selected_naics_code),
            has_substance_filter=False,
            has_material_filter=False,
        )
        run_eta = estimate_eta(run_request)

        executor = StepExecutor(num_steps=2)
        facilities_df = pd.DataFrame()
        samples_df = pd.DataFrame()
        executed_queries = []
        step_eta_by_label = {s.label: s for s in run_eta.step_estimates}

        def _record_step(step_info: dict) -> None:
            executed_queries.append(step_info)

        with executor.step(1, "Finding facilities...") as step:
            facilities_df, error, debug = execute_nearby_facilities_query(
                naics_code=selected_naics_code,
                region_code=context.region_code,
            )
            step_info = build_query_debug_entry(
                "Step 1: Facilities",
                debug,
                row_count=len(facilities_df),
                error=error,
            )
            _record_step(step_info)
            if error:
                step.error(f"Step 1 failed: {error}")
            elif not facilities_df.empty:
                step.success(f"Step 1: Found {len(facilities_df)} facilities")
            else:
                step.warning("Step 1: No facilities found")

        with executor.step(2, "Finding PFAS samples...") as step:
            samples_df, error, debug = execute_nearby_samples_query(
                naics_code=selected_naics_code,
                region_code=context.region_code,
                min_concentration=min_conc,
                max_concentration=max_conc,
                include_nondetects=include_nondetects,
            )
            step_info = build_query_debug_entry(
                "Step 2: Nearby Samples",
                debug,
                row_count=len(samples_df),
                error=error,
            )
            _record_step(step_info)
            if error:
                step.error(f"Step 2 failed: {error}")
            elif not samples_df.empty:
                step.success(f"Step 2: Found {len(samples_df)} PFAS samples")
            else:
                step.info("Step 2: No PFAS samples found")

        record_executed_query_batch(
            request=run_request,
            executed_queries=executed_queries,
            step_eta_by_label=step_eta_by_label,
        )
        boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)

        state.set("executed_queries", executed_queries)
        state.set_results({
            "facilities_df": facilities_df, "samples_df": samples_df,
            "industry_display": selected_industry_display, "boundaries": boundaries,
            "params_data": [
                build_industry_params(selected_industry_display),
                build_region_params(context.region_display, default_label="All Regions"),
                build_concentration_params(min_conc, max_conc, include_nondetects=False),
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
            metrics = [{"label": "Total Facilities", "value": len(facilities_df)}]
            if 'industryName' in facilities_df.columns:
                metrics.append({"label": "Industry Types", "value": facilities_df['industryName'].nunique()})
            facilities_table_df = add_naics_url_column(facilities_df)
            render_step_results("Step 1: Facilities", facilities_table_df, metrics, "View Facilities Data",
                display_columns=['facilityName', 'industryCode_url', 'industryName', 'facility'],
                download_filename=f"near_facilities_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_facilities",
                column_config={
                    "industryCode_url": st.column_config.LinkColumn(
                        "NAICS Code", display_text=r"code=(\d+)"
                    ),
                    "facility": st.column_config.LinkColumn(
                        "Facility", display_text=r"FRS-Facility\.(\d+)"
                    ),
                },
            )

        # Step 2: Samples
        if not samples_df.empty:
            metrics = [{"label": "Total Samples", "value": len(samples_df)}]
            if 'sp' in samples_df.columns:
                metrics.append({"label": "Unique Sample Points", "value": samples_df['sp'].nunique()})
            if 'max' in samples_df.columns:
                max_vals = pd.to_numeric(samples_df['max'], errors='coerce')
                if max_vals.notna().any():
                    metrics.append({"label": "Max Concentration", "value": f"{max_vals.max():.2f} ng/L"})

            samples_display = clean_unit_encoding(samples_df, columns=['unit', 'datedresults', 'results'])
            render_step_results("Step 2: PFAS Samples", samples_display, metrics, "View Samples Data",
                display_columns=['max', 'resultCount', 'datedresults', 'Materials', 'Type', 'spName', 'sp'],
                download_filename=f"near_facilities_samples_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_samples",
                show_stats=True,
                stats_column='max',
            )

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

        # Add facility links and NAICS code links
        if "facility" in facilities_gdf.columns:
            facilities_gdf = add_facility_link_column(facilities_gdf)
        if "industryCode" in facilities_gdf.columns:
            facilities_gdf = add_naics_link_column(facilities_gdf)

        facility_fields = [c for c in ["Facility ID", "facilityName", "industryName", "NAICS Code"] if c in facilities_gdf.columns]
        add_point_layer(map_obj, facilities_gdf,
            name=f'<span style="color:Blue;">{industry_display} ({len(facilities_gdf)})</span>',
            color='Blue', popup_fields=facility_fields, radius=FACILITY_MARKER_RADIUS,
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
        render_folium_map(map_obj)
        render_map_legend([
            "**Boundary** = Selected region",
            "**Blue markers** = Facilities of selected industry type",
            "**Orange markers** = PFAS sample points nearby"
        ])

    except Exception as e:
        st.error(f"Error creating map: {e}")
