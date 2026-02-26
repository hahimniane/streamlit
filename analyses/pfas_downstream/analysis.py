"""
PFAS Downstream Tracing Analysis (Query 5)
Trace contamination downstream from facilities of specific industry types
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
from streamlit_folium import st_folium

from analysis_registry import AnalysisContext
from analyses.pfas_downstream.queries import (
    execute_downstream_facilities_query,
    execute_downstream_streams_query,
    execute_downstream_samples_query,
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
from components.result_display import render_metrics_row, render_data_expander, clean_unit_encoding
from components.map_rendering import (
    FACILITY_MARKER_RADIUS,
    add_facility_link_column,
    add_naics_link_column,
    add_naics_url_column,
    create_base_map, add_boundary_layers, add_line_layer,
    add_grouped_point_layers, finalize_map, render_map_legend
)
from components.execute_button import render_execute_button, check_required_fields
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
    """Main function for PFAS Downstream Tracing analysis"""
    check_old_session_keys(['q5_conc_min', 'q5_conc_max', 'q5_has_results', 'q5_results'])

    st.markdown("""
    **What this analysis does:**
    - Finds facilities of a specific industry type in your selected region
    - Traces *downstream* through hydrological flow paths from those facilities
    - Identifies PFAS sample points downstream

    **3-Step Process:** Find facilities -> Trace downstream -> Identify PFAS samples

    **Use case:** Determine if PFAS contamination flows downstream from specific industries
    """)

    state = AnalysisState(context.analysis_key)
    state.init_if_missing("executed_queries", [])

    # === SIDEBAR PARAMETERS ===
    selected_naics_code, selected_industry_display = render_sidebar_industry_selector(
        analysis_key=context.analysis_key,
        heading="### Industry Type",
        caption="_Required: select an industry to trace downstream_",
        allow_empty=True,
        empty_label="Not Selected",
    )

    conc_filter = render_concentration_filter(context.analysis_key)

    # Execute button
    can_execute, missing = check_required_fields(industry=selected_naics_code)
    execute_clicked = render_execute_button(
        disabled=not can_execute,
        missing_fields=missing,
        help_text="Execute the downstream tracing analysis" if can_execute else None
    )

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

        if not selected_naics_code:
            st.error("**Missing required selections!** Please select: industry type")
        else:
            boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)

            params_data = [
                build_industry_params(selected_industry_display),
                build_region_params(context.region_display, default_label="All Regions"),
                build_concentration_params(min_conc, max_conc, include_nondetects=False),
                {"Parameter": "Include nondetects", "Value": "Yes" if include_nondetects else "No"},
            ]

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

            executor = StepExecutor(num_steps=3)
            facilities_df = pd.DataFrame()
            streams_df = pd.DataFrame()
            samples_df = pd.DataFrame()
            executed_queries = []
            step_eta_by_label = {s.label: s for s in run_eta.step_estimates}

            def _record_step(step_info: dict) -> None:
                executed_queries.append(step_info)

            with executor.step(1, "Finding facilities...") as step:
                facilities_df, error, debug = execute_downstream_facilities_query(
                    naics_code=selected_naics_code, region_code=context.region_code)
                step_info = build_query_debug_entry(
                    "Step 1: Facilities",
                    debug,
                    row_count=len(facilities_df) if facilities_df is not None else 0,
                    error=error,
                )
                _record_step(step_info)
                if error:
                    step.error(f"Step 1 failed: {error}")
                elif not facilities_df.empty:
                    step.success(f"Step 1: Found {len(facilities_df)} facilities")
                else:
                    step.warning("Step 1: No facilities found")

            with executor.step(2, "Tracing downstream streams...") as step:
                streams_df, error, debug = execute_downstream_streams_query(
                    naics_code=selected_naics_code, region_code=context.region_code)
                step_info = build_query_debug_entry(
                    "Step 2: Downstream Streams",
                    debug,
                    row_count=len(streams_df) if streams_df is not None else 0,
                    error=error,
                )
                _record_step(step_info)
                if error:
                    step.error(f"Step 2 failed: {error}")
                elif not streams_df.empty:
                    stream_count = streams_df["streamName"].dropna().nunique() if "streamName" in streams_df.columns else 0
                    step.success(f"Step 2: Found {len(streams_df)} flowlines ({stream_count} named streams)")
                else:
                    step.info("Step 2: No downstream flow paths found")

            with executor.step(3, "Finding downstream samples...") as step:
                samples_df, error, debug = execute_downstream_samples_query(
                    naics_code=selected_naics_code, region_code=context.region_code,
                    min_conc=min_conc, max_conc=max_conc, include_nondetects=include_nondetects)
                step_info = build_query_debug_entry(
                    "Step 3: Downstream Samples",
                    debug,
                    row_count=len(samples_df) if samples_df is not None else 0,
                    error=error,
                )
                _record_step(step_info)
                if error:
                    step.error(f"Step 3 failed: {error}")
                elif not samples_df.empty:
                    step.success(f"Step 3: Found {len(samples_df)} downstream samples")
                else:
                    step.info("Step 3: No downstream samples found")

            record_executed_query_batch(
                request=run_request,
                executed_queries=executed_queries,
                step_eta_by_label=step_eta_by_label,
            )
            state.set("executed_queries", executed_queries)
            state.set_results({
                "facilities_df": facilities_df, "streams_df": streams_df, "samples_df": samples_df,
                "boundaries": boundaries, "params_data": params_data,
                "query_region_code": context.region_code, "selected_industry": selected_industry_display,
                "executed_queries": executed_queries,
            })

    render_executed_queries(state.get("executed_queries", []))

    # === DISPLAY RESULTS ===
    if state.has_results:
        results = state.get_results()
        facilities_df = results.get('facilities_df', pd.DataFrame())
        streams_df = results.get('streams_df', pd.DataFrame())
        samples_df = results.get('samples_df', pd.DataFrame())
        boundaries = results.get('boundaries', {})
        params_data = results.get('params_data', [])
        query_region_code = results.get('query_region_code')

        st.markdown("---")
        render_parameter_table(params_data)

        st.markdown("---")
        st.markdown("### Query Results")
        st.markdown("---")

        # Step 1: Facilities
        if not facilities_df.empty:
            st.markdown("### Step 1: Facilities")
            metrics = [{"label": "Total Facilities", "value": len(facilities_df)}]
            if 'industryName' in facilities_df.columns:
                metrics.append({"label": "Industry Types", "value": facilities_df['industryName'].nunique()})
            render_metrics_row(metrics, num_columns=2)
            facilities_table_df = add_naics_url_column(facilities_df)
            render_data_expander("View Facilities Data", facilities_table_df,
                display_columns=['facilityName', 'industryName', 'industryCode_url', 'facility'],
                download_filename=f"downstream_facilities_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_facilities",
                column_config={"industryCode_url": st.column_config.LinkColumn(
                    "NAICS Code", display_text=r"code=(\d+)"
                )})

        # Step 2: Streams
        if not streams_df.empty:
            st.markdown("### Step 2: Downstream Streams")
            stream_names = streams_df["streamName"].dropna().unique() if "streamName" in streams_df.columns else []
            render_metrics_row([
                {"label": "Total Flowlines", "value": len(streams_df)},
                {"label": "Named Streams", "value": len(stream_names)}
            ], num_columns=2)
            render_data_expander("View Streams Data", streams_df,
                display_columns=['streamName', 'fl_type', 'downstream_flowline'],
                download_filename=f"downstream_streams_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_streams")

        # Step 3: Samples
        if not samples_df.empty:
            st.markdown("### Step 3: Downstream Samples")
            metrics = [{"label": "Total Samples", "value": len(samples_df)}]
            if 'samplePoint' in samples_df.columns:
                metrics.append({"label": "Unique Sample Points", "value": samples_df['samplePoint'].nunique()})
            if 'Max' in samples_df.columns:
                max_vals = pd.to_numeric(samples_df['Max'], errors='coerce')
                if max_vals.notna().any():
                    metrics.append({"label": "Max Concentration", "value": f"{max_vals.max():.2f} ng/L"})
            render_metrics_row(metrics, num_columns=3)
            render_data_expander("View Samples Data", samples_df,
                display_columns=['Max', 'resultCount', 'unit', 'results', 'samplePoint', 'sample'],
                download_filename=f"downstream_samples_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_samples",
                show_stats=True, stats_column='Max')

        # Map
        _render_map(facilities_df, streams_df, samples_df, boundaries, context)
    else:
        st.info("Select an industry type in the sidebar, then click 'Execute Query' to run the analysis")


def _render_map(facilities_df, streams_df, samples_df, boundaries, context) -> None:
    """Render the interactive map."""
    has_facilities = not facilities_df.empty and 'facWKT' in facilities_df.columns
    has_streams = not streams_df.empty and 'dsflWKT' in streams_df.columns
    has_samples = not samples_df.empty and 'spWKT' in samples_df.columns

    if not has_facilities and not has_streams and not has_samples:
        return

    st.markdown("---")
    st.markdown("### Interactive Map")

    facilities_gdf = create_geodataframe(facilities_df, 'facWKT') if has_facilities else None
    streams_gdf = create_geodataframe(streams_df, 'dsflWKT') if has_streams else None
    samples_gdf = create_geodataframe(samples_df, 'spWKT') if has_samples else None

    # Add facility links and NAICS code links
    if facilities_gdf is not None and 'facility' in facilities_gdf.columns:
        facilities_gdf = add_facility_link_column(facilities_gdf)
    if facilities_gdf is not None and 'industryCode' in facilities_gdf.columns:
        facilities_gdf = add_naics_link_column(facilities_gdf)

    # Clean sample data
    if samples_gdf is not None:
        samples_gdf = clean_unit_encoding(samples_gdf)

    map_obj = create_base_map(gdf_list=[samples_gdf, facilities_gdf, streams_gdf], zoom=8)
    add_boundary_layers(map_obj, boundaries, context.region_code, warn_fn=st.warning)

    # Add samples with custom styling
    if samples_gdf is not None and not samples_gdf.empty:
        def _sample_style(feature):
            props = (feature or {}).get("properties", {}) or {}
            max_val = props.get("Max")
            is_nondetect = max_val in ["non-detect", "http://w3id.org/coso/v1/contaminoso#non-detect"]
            if not is_nondetect:
                try:
                    is_nondetect = float(max_val) == 0
                except:
                    pass
            radius = 4
            if not is_nondetect:
                try:
                    v = float(max_val)
                    radius = 4 if v < 40 else (v / 16 if v < 160 else 12)
                except:
                    pass
            return {"radius": max(3, min(12, radius)), "opacity": 0.3, "color": "Black" if is_nondetect else "DimGray"}

        samples_gdf.explore(m=map_obj, name='<span style="color:DarkOrange;">Samples</span>',
            color="DarkOrange", marker_kwds=dict(radius=6), marker_type="circle_marker",
            popup=True, popup_kwds={"max_height": 500, "max_width": 650},
            style_kwds=dict(style_function=_sample_style))

    # Add streams
    if streams_gdf is not None and not streams_gdf.empty:
        stream_popup = [c for c in ["streamName", "fl_type", "downstream_flowline"] if c in streams_gdf.columns]
        add_line_layer(map_obj, streams_gdf, '<span style="color:LightSkyBlue;">Streams</span>',
                       'LightSkyBlue', popup_fields=stream_popup)

    # Add facilities
    if facilities_gdf is not None and not facilities_gdf.empty:
        fields = [c for c in ["facility_link", "facilityName", "industryName", "industryCode_link"] if c in facilities_gdf.columns]
        add_grouped_point_layers(map_obj, facilities_gdf, 'industryName', popup_fields=fields, radius=FACILITY_MARKER_RADIUS,
                                 popup_kwds={"max_width": 900, "parse_html": True})

    finalize_map(map_obj)
    st_folium(map_obj, width=None, height=700, returned_objects=[])
    render_map_legend([
        "**Boundary outline** = Selected region",
        "**Orange circles** = PFAS samples downstream",
        "**Light blue lines** = Downstream flow paths",
        "**Purple/pink markers** = Facilities (by industry)"
    ])

    # Stream names
    if streams_gdf is not None and 'streamName' in streams_gdf.columns:
        names = sorted(streams_gdf['streamName'].dropna().unique())
        if names:
            with st.expander(f"Stream Names ({len(names)} unique)"):
                st.write(", ".join(names))
