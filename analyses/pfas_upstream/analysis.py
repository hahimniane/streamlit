"""
PFAS Upstream Tracing Analysis (Query 1)
Trace contamination upstream to identify potential sources
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from analysis_registry import AnalysisContext
from analyses.pfas_upstream.queries import run_upstream
from filters.industry import render_sidebar_industry_selector
from filters.substance import get_cached_substances_with_labels
from filters.material import get_cached_material_types_with_labels
from filters.concentration import render_concentration_filter, apply_concentration_filter

# Shared components
from core.boundary import fetch_boundaries
from core.geometry import create_geodataframe
from components.parameter_display import (
    build_concentration_params,
    render_parameter_table,
)
from components.result_display import render_step_results
from components.map_rendering import (
    FACILITY_MARKER_RADIUS,
    add_facility_link_column,
    add_naics_link_column,
    add_naics_url_column,
    create_base_map, add_boundary_layers, add_point_layer,
    add_line_layer, add_grouped_point_layers, finalize_map, render_map_legend, render_folium_map
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
    """Main function for PFAS Upstream Tracing analysis"""
    # Check for old session state keys
    check_old_session_keys(['conc_min', 'conc_max', 'has_results', 'query_results', 'selected_substance', 'selected_material_type'])

    st.markdown("""
    **What this analysis does:**
    - Finds water samples with PFAS contamination in your selected region
    - Traces upstream through hydrological flow paths
    - Identifies industrial facilities that may be contamination sources

    **3-Step Process:** Find contamination -> Trace upstream -> Identify potential sources
    """)

    # Initialize state manager
    state = AnalysisState(context.analysis_key)
    state.init_if_missing('selected_substance', None)
    state.init_if_missing('selected_material_type', None)
    state.init_if_missing('executed_queries', [])

    # === SIDEBAR PARAMETERS ===
    st.sidebar.markdown("### Query Parameters")

    # Substance selector
    is_subdivision = len(context.region_code) > 5 if context.region_code else False

    substances_view = (
        get_cached_substances_with_labels(context.region_code, is_subdivision)
        if context.region_code
        else pd.DataFrame()
    )

    st.sidebar.markdown("### PFAS Substance")
    substance_map = {}
    if not substances_view.empty:
        for _, row in substances_view.iterrows():
            name = row["display_name"]
            uri = row["substance"]
            if name not in substance_map or uri.endswith("_A"):
                substance_map[name] = uri

    selected_substance_display = st.sidebar.selectbox(
        "Select PFAS Substance (Optional)",
        ["-- All Substances --"] + sorted(substance_map.keys()),
        help="Select a specific PFAS compound to analyze, or leave as 'All Substances'",
    )

    selected_substance_uri = None
    selected_substance_name = None
    if selected_substance_display != "-- All Substances --":
        selected_substance_name = selected_substance_display
        selected_substance_uri = substance_map.get(selected_substance_display)

    st.sidebar.markdown("---")

    # Material type selector
    st.sidebar.markdown("### Sample Material Type")
    material_types_view = (
        get_cached_material_types_with_labels(context.region_code, is_subdivision)
        if context.region_code
        else pd.DataFrame()
    )

    material_type_map = {}
    if not material_types_view.empty:
        for _, row in material_types_view.iterrows():
            material_type_map[row["display_name"]] = row["matType"]

    selected_material_display = st.sidebar.selectbox(
        "Select Material Type (Optional)",
        ["-- All Material Types --"] + list(material_type_map.keys()),
        help="Select the type of sample material analyzed",
    )

    selected_material_uri = None
    selected_material_name = None
    if selected_material_display != "-- All Material Types --":
        selected_material_name = selected_material_display
        selected_material_uri = material_type_map.get(selected_material_display)

    st.sidebar.markdown("---")

    # Optional industry selector (applies to Step 3 facilities only)
    selected_naics_code, selected_industry_display = render_sidebar_industry_selector(
        analysis_key=context.analysis_key,
        heading="### Industry Type",
        caption="_Optional: filter Step 3 upstream facilities by industry_",
        allow_empty=True,
        empty_label="All Industries",
    )

    st.sidebar.markdown("---")

    # Concentration filter
    conc_filter = render_concentration_filter(context.analysis_key, default_max=500)

    # Execute button
    county_selected = context.selected_county_code is not None
    execute_clicked = render_execute_button(
        disabled=not county_selected,
        missing_fields=["county"] if not county_selected else None,
        help_text="Execute the upstream tracing analysis" if county_selected else None
    )

    preview_request = build_eta_request(
        analysis_key=context.analysis_key,
        region_code=context.region_code,
        state_code=context.selected_state_code,
        min_conc=conc_filter.min_concentration,
        max_conc=conc_filter.max_concentration,
        include_nondetects=conc_filter.include_nondetects,
        naics_prefix2=naics_prefix2_from_code(selected_naics_code),
        has_substance_filter=selected_substance_uri is not None,
        has_material_filter=selected_material_uri is not None,
    )
    render_simple_eta(estimate_eta(preview_request))

    # === QUERY EXECUTION ===
    if execute_clicked:
        min_conc, max_conc, include_nondetects = apply_concentration_filter(context.analysis_key)

        if not context.selected_state_code:
            st.error("**State selection is required!** Please select a state before executing the query.")
        else:
            # Build parameters
            params_data = [
                {"Parameter": "PFAS Substance", "Value": selected_substance_name or "All Substances"},
                {"Parameter": "Material Type", "Value": selected_material_name or "All Material Types"},
                {"Parameter": "Industry Type (Step 3 Facilities)", "Value": selected_industry_display},
                build_concentration_params(min_conc, max_conc, include_nondetects),
                {"Parameter": "Geographic Region", "Value": context.region_display},
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
                has_substance_filter=selected_substance_uri is not None,
                has_material_filter=selected_material_uri is not None,
            )
            run_eta = estimate_eta(run_request)

            boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)
            executor = StepExecutor(num_steps=3)
            samples_df = pd.DataFrame()
            upstream_s2_df = pd.DataFrame()
            upstream_flowlines_df = pd.DataFrame()
            facilities_df = pd.DataFrame()
            executed_queries = []

            with st.spinner("Running upstream tracing (3 federation queries)..."):
                (
                    samples_df,
                    upstream_s2_df,
                    upstream_flowlines_df,
                    facilities_df,
                    executed_queries,
                    err,
                ) = run_upstream(
                    selected_substance_uri,
                    selected_material_uri,
                    min_conc,
                    max_conc,
                    context.region_code,
                    include_nondetects=include_nondetects,
                    naics_code=selected_naics_code,
                )

            with executor.step(1, "Step 1") as step:
                if not samples_df.empty:
                    step.success(f"Found {len(samples_df)} PFAS samples")
                else:
                    step.warning("No PFAS samples found")
            with executor.step(2, "Step 2") as step:
                n_fl = len(upstream_flowlines_df)
                if n_fl:
                    step.success(f"Traced {n_fl} upstream flowlines")
                else:
                    step.info("No upstream flow paths found")
            with executor.step(3, "Step 3") as step:
                if not facilities_df.empty:
                    step.success(f"Found {len(facilities_df)} facilities")
                else:
                    step.info("No facilities found")
            if err:
                st.error(err)

            step_eta_by_label = {s.label: s for s in run_eta.step_estimates}
            record_executed_query_batch(
                request=run_request,
                executed_queries=executed_queries,
                step_eta_by_label=step_eta_by_label,
            )

            state.set('executed_queries', executed_queries)
            # Store results
            state.set_results({
                'samples_df': samples_df,
                'upstream_s2_df': upstream_s2_df,
                'upstream_flowlines_df': upstream_flowlines_df,
                'facilities_df': facilities_df,
                'boundaries': boundaries,
                'params_data': params_data,
                'query_region_code': context.region_code,
                'selected_material_name': selected_material_name,
                'executed_queries': executed_queries,
            })

    render_executed_queries(state.get('executed_queries', []))

    # === DISPLAY RESULTS ===
    if state.has_results:
        results = state.get_results()
        samples_df = results.get('samples_df', pd.DataFrame())
        upstream_s2_df = results.get('upstream_s2_df', pd.DataFrame())
        upstream_flowlines_df = results.get('upstream_flowlines_df', pd.DataFrame())
        facilities_df = results.get('facilities_df', pd.DataFrame())
        boundaries = results.get('boundaries', {})
        params_data = results.get('params_data', [])
        query_region_code = results.get('query_region_code')
        saved_material_name = results.get('selected_material_name')
        st.markdown("---")
        render_parameter_table(params_data)
        st.markdown("---")
        st.markdown("### Query Results")
        st.markdown("---")

        # Step 1 Results
        if not samples_df.empty:
            metrics = [{"label": "Total Samples", "value": len(samples_df)}]
            if 'sp' in samples_df.columns:
                metrics.append({"label": "Unique Sample Points", "value": samples_df['sp'].nunique()})
            if 'matType' in samples_df.columns:
                metrics.append({"label": "Material Type", "value": saved_material_name or "All"})
            render_step_results("Step 1: PFAS Samples", samples_df, metrics, "View PFAS Samples Data",
                download_filename=f"contaminated_samples_{query_region_code}.csv",
                download_key=f"download_{context.analysis_key}_samples",
            )

        # Step 2 Results (notebook-style returns flowlines only; default returns upstream_s2_df)
        if not upstream_s2_df.empty or not upstream_flowlines_df.empty:
            st.markdown("### Step 2: Upstream Flow Paths")
            step2_count = len(upstream_s2_df) if not upstream_s2_df.empty else len(upstream_flowlines_df)
            st.metric("Total Upstream Connections", step2_count)

        # Step 3 Results
        if not facilities_df.empty:
            metrics = [{"label": "Total Facilities", "value": len(facilities_df)}]
            if 'industryName' in facilities_df.columns:
                metrics.append({"label": "Industry Types", "value": facilities_df['industryName'].nunique()})
            facilities_table_df = add_naics_url_column(facilities_df)
            render_step_results("Step 3: Potential Source Facilities", facilities_table_df, metrics, "View Facilities Data",
                display_columns=['facilityName', 'industryCode_url', 'industryName', 'facility'],
                download_filename=f"upstream_facilities_{query_region_code}.csv",
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

            # Industry breakdown
            if 'industryName' in facilities_df.columns:
                _render_industry_breakdown(facilities_df)

        # Map
        _render_map(samples_df, facilities_df, upstream_s2_df, upstream_flowlines_df, boundaries, context)


def _render_industry_breakdown(facilities_df: pd.DataFrame) -> None:
    """Render the industry breakdown expander."""
    with st.expander("Industry Breakdown", expanded=False):
        flat_data = facilities_df.copy()
        flat_data['industryName'] = flat_data['industryName'].astype(str).str.strip()

        if 'industryCode' in flat_data.columns:
            flat_data['code_clean'] = flat_data['industryCode'].apply(
                lambda x: x.split('-')[-1] if isinstance(x, str) and '-' in x else '')
            flat_data['code_len'] = flat_data['code_clean'].str.len()
            flat_data = flat_data.sort_values(['facility', 'code_len'], ascending=[True, False])
            flat_data = flat_data.drop_duplicates(subset=['facility'], keep='first')
            flat_data['display_name'] = flat_data.apply(
                lambda r: f"{r['industryName']} ({r['code_clean']})" if r['code_clean'] else r['industryName'], axis=1)
        else:
            flat_data['display_name'] = flat_data['industryName']
            flat_data = flat_data.drop_duplicates(subset=['facility'], keep='first')

        summary = flat_data.groupby('display_name').agg(Facilities=('facility', 'nunique')).reset_index()
        total = flat_data['facility'].nunique()
        summary['Percentage'] = (summary['Facilities'] / total * 100).map('{:.1f}%'.format) if total > 0 else "0.0%"
        summary.columns = ['Industry', 'Facilities', 'Percentage']
        st.dataframe(summary.sort_values('Facilities', ascending=False).reset_index(drop=True),
                     use_container_width=True, hide_index=True)


def _render_map(samples_df, facilities_df, upstream_s2_df, upstream_flowlines_df, boundaries, context) -> None:
    """Render the interactive map."""
    has_samples = not samples_df.empty and 'spWKT' in samples_df.columns
    has_facilities = not facilities_df.empty and 'facWKT' in facilities_df.columns

    if not has_samples and not has_facilities:
        return

    st.markdown("---")
    st.markdown("### Interactive Map")

    samples_gdf = create_geodataframe(samples_df, 'spWKT') if has_samples else None
    facilities_gdf = create_geodataframe(facilities_df, 'facWKT') if has_facilities else None

    # Handle flowlines
    flowlines_gdf = None
    if not upstream_flowlines_df.empty and 'upstream_flowlineWKT' in upstream_flowlines_df.columns:
        flowlines_gdf = create_geodataframe(upstream_flowlines_df, 'upstream_flowlineWKT')
    elif not upstream_s2_df.empty and 'upstream_flowlineWKT' in upstream_s2_df.columns:
        flowlines_gdf = create_geodataframe(upstream_s2_df, 'upstream_flowlineWKT')

    if samples_gdf is None and facilities_gdf is None and flowlines_gdf is None:
        return

    map_obj = create_base_map(gdf_list=[samples_gdf, facilities_gdf, flowlines_gdf], zoom=8)
    add_boundary_layers(map_obj, boundaries, context.region_code)

    if flowlines_gdf is not None and not flowlines_gdf.empty:
        add_line_layer(map_obj, flowlines_gdf, '<span style="color:DodgerBlue;">Upstream Flowlines</span>',
                       'DodgerBlue', weight=3, opacity=0.5)

    if samples_gdf is not None and not samples_gdf.empty:
        fields = [c for c in ["sp", "result_value", "substance", "matType", "regionURI"] if c in samples_gdf.columns]
        add_point_layer(map_obj, samples_gdf, '<span style="color:DarkOrange;">PFAS Samples</span>',
                        'DarkOrange', popup_fields=fields, radius=8)

    if facilities_gdf is not None and not facilities_gdf.empty:
        group_col = 'industryName'
        for col in ['industrySubsectorName', 'industryGroupName']:
            if col in facilities_gdf.columns and facilities_gdf[col].notna().any():
                group_col = col
                break
        if 'industryCode' in facilities_gdf.columns:
            facilities_gdf = add_naics_link_column(facilities_gdf)
        if 'facility' in facilities_gdf.columns:
            facilities_gdf = add_facility_link_column(facilities_gdf)
        fields = [c for c in ["facilityName", "industryName", "NAICS Code", "Facility ID"] if c in facilities_gdf.columns]
        add_grouped_point_layers(map_obj, facilities_gdf, group_col, popup_fields=fields, radius=FACILITY_MARKER_RADIUS,
                                 popup_kwds={"parse_html": True},
                                 tooltip_kwds={"parse_html": True})

    finalize_map(map_obj)
    render_folium_map(map_obj)
    render_map_legend([
        "**Orange circles** = PFAS sample locations",
        "**Blue lines** = Upstream flow paths",
        "**Colored markers** = Upstream facilities (by industry)",
        "**Boundary outline** = Selected region"
    ])
