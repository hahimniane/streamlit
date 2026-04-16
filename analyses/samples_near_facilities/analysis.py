"""
Samples Near Facilities Analysis (Query 2)
Find PFAS samples near facilities of a specific industry type
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
from filters.substance import render_sidebar_substance_selector

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
from components.result_display import render_step_results
from components.map_rendering import (
    FACILITY_MARKER_RADIUS,
    COLOR_SAMPLE, FACILITY_COLORS_REDS,
    add_facility_link_column,
    add_naics_link_column,
    add_naics_url_column,
    create_base_map,
    add_boundary_layers,
    add_point_layer,
    add_sample_layer,
    finalize_map,
    render_map_legend,
    render_folium_map,
)
from components.sample_popup import (
    aggregate_sample_popups,
    aggregate_sample_popups_lite,
    SAMPLE_POPUP_FIELDS,
    SAMPLE_POPUP_FIELDS_LITE,
    SAMPLE_POPUP_KWDS,
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
    st.sidebar.markdown("### Query Parameters")

    selected_substance_uri, selected_substance_name = render_sidebar_substance_selector(
        region_code=context.region_code,
        analysis_key=context.analysis_key,
    )

    st.sidebar.markdown("---")

    selected_naics_code, selected_industry_display = render_sidebar_industry_selector(
        analysis_key=context.analysis_key,
        heading="### Industry Type",
        caption="_Select an industry or a state (at minimum) to run the analysis_",
        allow_empty=True,
        empty_label="All Industries",
    )

    st.sidebar.markdown("---")

    conc_filter = render_concentration_filter(context.analysis_key)

    has_filter = bool(context.selected_state_code or selected_naics_code)
    if not has_filter:
        st.sidebar.warning("Please select at least a **state** or an **industry type** to run this analysis.")
    execute_clicked = render_execute_button(
        help_text="Execute the nearby facilities analysis",
        disabled=not has_filter,
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
            has_substance_filter=selected_substance_uri is not None,
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

        if facilities_df.empty:
            st.warning("No facilities found — skipping nearby samples query.")
            samples_df = pd.DataFrame()
        else:
            with executor.step(2, "Finding PFAS samples...") as step:
                samples_df, error, debug = execute_nearby_samples_query(
                    naics_code=selected_naics_code,
                    region_code=context.region_code,
                    min_concentration=min_conc,
                    max_concentration=max_conc,
                    include_nondetects=include_nondetects,
                    substance_uri=selected_substance_uri,
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

        # Aggregate raw samples for map popups
        # Use lightweight popups above 20K observations to keep the map responsive
        _LITE_THRESHOLD = 20_000
        use_lite = len(samples_df) > _LITE_THRESHOLD
        if samples_df.empty:
            samples_agg_df = pd.DataFrame()
        elif use_lite:
            st.info(
                f"Large dataset ({len(samples_df):,} observations) — "
                "using compact per-substance summary popups for map performance."
            )
            samples_agg_df = aggregate_sample_popups_lite(samples_df)
        else:
            samples_agg_df = aggregate_sample_popups(samples_df)

        record_executed_query_batch(
            request=run_request,
            executed_queries=executed_queries,
            step_eta_by_label=step_eta_by_label,
        )
        boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)

        state.set("executed_queries", executed_queries)
        state.set_results({
            "facilities_df": facilities_df, "samples_df": samples_df,
            "samples_agg_df": samples_agg_df,
            "use_lite_popups": use_lite,
            "industry_display": selected_industry_display, "boundaries": boundaries,
            "params_data": [
                build_industry_params(selected_industry_display),
                build_region_params(context.region_display, default_label="All Regions"),
                {"Parameter": "PFAS Substance", "Value": selected_substance_name or "All Substances"},
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
        samples_agg_df = results.get("samples_agg_df", pd.DataFrame())
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
            n_sample_points = samples_df['samplePoint'].nunique() if 'samplePoint' in samples_df.columns else len(samples_agg_df)
            metrics = [
                {"label": "Total Observations", "value": len(samples_df)},
                {"label": "Unique Sample Points", "value": n_sample_points},
            ]
            if not samples_agg_df.empty and "overall_max_result" in samples_agg_df.columns:
                max_vals = pd.to_numeric(samples_agg_df["overall_max_result"], errors="coerce")
                if max_vals.notna().any():
                    metrics.append({"label": "Max Concentration", "value": f"{max_vals.max():.2f} ng/L"})

            render_step_results("Step 2: PFAS Samples", samples_df, metrics, "View Samples Data",
                download_filename=f"near_facilities_samples_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_samples",
            )

        # Map
        use_lite = results.get("use_lite_popups", False)
        _render_map(facilities_df, samples_agg_df, industry_display, boundaries, query_region_code, context, use_lite)

        if facilities_df.empty and samples_df.empty:
            st.warning("No results found. Try a different industry type or region.")
    else:
        st.info("Select parameters in the sidebar and click 'Execute Query' to run the analysis")


def _render_map(facilities_df, samples_agg_df, industry_display, boundaries, query_region_code, context, use_lite: bool = False) -> None:
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

        samples_gdf = None
        if not samples_agg_df.empty and 'spWKT' in samples_agg_df.columns:
            samples_gdf = create_geodataframe(samples_agg_df, 'spWKT')

        map_obj = create_base_map(gdf_list=[facilities_gdf] + ([samples_gdf] if samples_gdf is not None else []), zoom=8)
        add_boundary_layers(map_obj, boundaries, query_region_code)

        # Add facility links and NAICS code links
        if "facility" in facilities_gdf.columns:
            facilities_gdf = add_facility_link_column(facilities_gdf)
        if "industryCode" in facilities_gdf.columns:
            facilities_gdf = add_naics_link_column(facilities_gdf)

        facility_color = FACILITY_COLORS_REDS[3]  # #cb181d — strong red
        facility_fields = [c for c in ["Facility ID", "facilityName", "industryName", "NAICS Code"] if c in facilities_gdf.columns]
        add_point_layer(map_obj, facilities_gdf,
            name=f'<span style="color:{facility_color};">{industry_display} ({len(facilities_gdf)})</span>',
            color=facility_color, popup_fields=facility_fields, radius=FACILITY_MARKER_RADIUS,
            popup_kwds={"max_width": 650, "parse_html": True},
            tooltip_kwds={"sticky": True, "parse_html": True})

        # Add samples with popup (PuOr concentration palette)
        if samples_gdf is not None and not samples_gdf.empty:
            popup_fields = SAMPLE_POPUP_FIELDS_LITE if use_lite else SAMPLE_POPUP_FIELDS
            popup_kwds = SAMPLE_POPUP_KWDS if not use_lite else {"max_width": 500, "max_height": 400, "parse_html": True}
            add_sample_layer(map_obj, samples_gdf,
                popup_fields=popup_fields, popup_kwds=popup_kwds,
                name=f'<span style="color:{COLOR_SAMPLE};">PFAS Samples ({len(samples_gdf)})</span>',
                radius=6)

        finalize_map(map_obj)
        render_folium_map(map_obj)
        render_map_legend([
            "**Boundary** = Selected region",
            "**Red markers** = Facilities of selected industry type",
            "**Orange circles** = PFAS sample points nearby"
        ])

    except Exception as e:
        st.error(f"Error creating map: {e}")
