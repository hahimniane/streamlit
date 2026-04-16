"""
Aquifer-Connected Wells Analysis
Find PFAS-tested sample points, connected aquifers, and water wells
(Maine MGS) that may be at risk through shared aquifer connectivity.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
from folium.plugins import StripePattern

from analysis_registry import AnalysisContext
from analyses.aquifer_wells.queries import (
    execute_aquifer_samples_query,
    execute_aquifer_aquifers_query,
    execute_aquifer_wells_query,
)
from filters.substance import render_sidebar_substance_selector
from filters.concentration import render_concentration_filter, apply_concentration_filter

from core.boundary import fetch_boundaries
from core.geometry import create_geodataframe
from core.sparql import build_query_debug_entry
from components.parameter_display import (
    build_concentration_params,
    build_region_params,
    render_parameter_table,
)
from components.result_display import render_step_results
from components.map_rendering import (
    create_base_map, add_boundary_layers, add_point_layer,
    add_sample_layer, finalize_map, render_map_legend, render_folium_map,
    COLOR_AQUIFER, COLOR_WELL, COLOR_SAMPLE,
)
from components.execute_button import render_execute_button, check_required_fields
from components.analysis_state import AnalysisState, check_old_session_keys
from components.step_execution import StepExecutor
from components.query_debug import render_executed_queries
from components.eta_display import render_simple_eta
from components.sample_popup import (
    aggregate_sample_popups,
    aggregate_sample_popups_lite,
    SAMPLE_POPUP_FIELDS,
    SAMPLE_POPUP_FIELDS_LITE,
    SAMPLE_POPUP_KWDS,
)
from core.runtime_eta import (
    build_eta_request,
    estimate_eta,
    record_executed_query_batch,
)


def main(context: AnalysisContext) -> None:
    """Main function for Aquifer-Connected Wells analysis."""
    check_old_session_keys([])

    st.markdown("""
    **What this analysis does:**
    - Finds PFAS-tested sample points in your selected region
    - Identifies aquifers spatially connected to those sample points
    - Finds water wells connected to those same aquifers that may be at risk

    **Process:** Find tested samples -> Identify connected aquifers -> Find at-risk wells

    **Use case:** Determine if water wells may be at risk through shared aquifer connectivity to PFAS contamination


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

    conc_filter = render_concentration_filter(context.analysis_key, default_max=500)

    can_execute, missing = check_required_fields(state=context.selected_state_code, county=context.selected_county_code)
    execute_clicked = render_execute_button(
        disabled=not can_execute,
        missing_fields=missing,
        help_text="Execute the aquifer-connected wells analysis" if can_execute else None,
    )

    preview_request = build_eta_request(
        analysis_key=context.analysis_key,
        region_code=context.region_code,
        state_code=context.selected_state_code,
        min_conc=conc_filter.min_concentration,
        max_conc=conc_filter.max_concentration,
        include_nondetects=conc_filter.include_nondetects,
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
            has_substance_filter=selected_substance_uri is not None,
            has_material_filter=False,
        )
        run_eta = estimate_eta(run_request)

        executor = StepExecutor(num_steps=3)
        samples_raw_df = pd.DataFrame()
        aquifers_df = pd.DataFrame()
        wells_df = pd.DataFrame()
        executed_queries = []
        step_eta_by_label = {s.label: s for s in run_eta.step_estimates}

        query_args = dict(
            region_code=context.region_code,
            substance_uri=selected_substance_uri,
            min_conc=min_conc,
            max_conc=max_conc,
            include_nondetects=include_nondetects,
        )

        with executor.step(1, "Finding sample observations...") as step:
            samples_raw_df, error, debug = execute_aquifer_samples_query(**query_args)
            step_info = build_query_debug_entry(
                "Step 1: Sample Observations", debug,
                row_count=len(samples_raw_df), error=error,
            )
            executed_queries.append(step_info)
            if error:
                step.error(f"Step 1 failed: {error}")
            elif not samples_raw_df.empty:
                n_sp = samples_raw_df["samplePoint"].nunique() if "samplePoint" in samples_raw_df.columns else 0
                step.success(f"Step 1: Found {len(samples_raw_df)} observations across {n_sp} sample points")
            else:
                step.warning("Step 1: No sample observations found")

        if samples_raw_df.empty:
            st.warning("No sample points found — skipping aquifer and well queries. "
                       "Without observations, aquifer/well results would be misleading.")
        else:
            with executor.step(2, "Finding connected aquifers...") as step:
                aquifers_df, error, debug = execute_aquifer_aquifers_query(**query_args)
                step_info = build_query_debug_entry(
                    "Step 2: Aquifers", debug,
                    row_count=len(aquifers_df), error=error,
                )
                executed_queries.append(step_info)
                if error:
                    step.error(f"Step 2 failed: {error}")
                elif not aquifers_df.empty:
                    step.success(f"Step 2: Found {len(aquifers_df)} aquifer(s)")
                else:
                    step.warning("Step 2: No aquifers found")

            with executor.step(3, "Finding connected wells...") as step:
                wells_df, error, debug = execute_aquifer_wells_query(**query_args)
                step_info = build_query_debug_entry(
                    "Step 3: Connected Wells", debug,
                    row_count=len(wells_df), error=error,
                )
                executed_queries.append(step_info)
                if error:
                    step.error(f"Step 3 failed: {error}")
                elif not wells_df.empty:
                    step.success(f"Step 3: Found {len(wells_df)} well(s)")
                else:
                    step.warning("Step 3: No connected wells found")

        # Aggregate raw samples for map popups
        _LITE_THRESHOLD = 20_000
        use_lite = len(samples_raw_df) > _LITE_THRESHOLD
        if samples_raw_df.empty:
            samples_agg_df = pd.DataFrame()
        elif use_lite:
            st.info(
                f"Large dataset ({len(samples_raw_df):,} observations) — "
                "using compact per-substance summary popups for map performance."
            )
            samples_agg_df = aggregate_sample_popups_lite(samples_raw_df)
        else:
            samples_agg_df = aggregate_sample_popups(samples_raw_df)

        boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)

        record_executed_query_batch(
            request=run_request,
            executed_queries=executed_queries,
            step_eta_by_label=step_eta_by_label,
        )
        state.set("executed_queries", executed_queries)
        state.set_results({
            "samples_raw_df": samples_raw_df,
            "samples_agg_df": samples_agg_df,
            "use_lite_popups": use_lite,
            "aquifers_df": aquifers_df,
            "wells_df": wells_df,
            "boundaries": boundaries,
            "params_data": [
                {"Parameter": "PFAS Substance", "Value": selected_substance_name or "All Substances"},
                build_concentration_params(min_conc, max_conc, include_nondetects),
                build_region_params(context.region_display, default_label="All Regions"),
            ],
            "query_region_code": context.region_code,
            "executed_queries": executed_queries,
        })

    render_executed_queries(state.get("executed_queries", []))

    # === DISPLAY RESULTS ===
    if state.has_results:
        results = state.get_results()
        samples_raw_df = results.get("samples_raw_df", pd.DataFrame())
        samples_agg_df = results.get("samples_agg_df", pd.DataFrame())
        aquifers_df = results.get("aquifers_df", pd.DataFrame())
        wells_df = results.get("wells_df", pd.DataFrame())
        boundaries = results.get("boundaries", {})
        params_data = results.get("params_data", [])
        query_region_code = results.get("query_region_code")

        st.markdown("---")
        render_parameter_table(params_data)
        st.markdown("### Query Results")
        st.markdown("---")

        if not samples_agg_df.empty:
            metrics = [{"label": "Sample Points", "value": len(samples_agg_df)}]
            if "overall_max_result" in samples_agg_df.columns:
                vals = pd.to_numeric(samples_agg_df["overall_max_result"], errors="coerce")
                if vals.notna().any():
                    metrics.append({"label": "Max Concentration", "value": f"{vals.max():.2f} ng/L"})
            render_step_results(
                "Sample Points", samples_raw_df, metrics,
                "View Sample Observations Data",
                download_filename=f"aquifer_samples_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_samples",
            )

        if not aquifers_df.empty:
            render_step_results(
                "Connected Aquifers", aquifers_df,
                [{"label": "Aquifers", "value": len(aquifers_df)}],
                "View Aquifers Data",
                display_columns=["aquifer"],
                download_filename=f"aquifer_aquifers_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_aquifers",
            )

        if not wells_df.empty:
            render_step_results(
                "Connected Wells", wells_df,
                [{"label": "Connected Wells", "value": len(wells_df)}],
                "View Wells Data",
                display_columns=["welllabel", "Well Use", "Well Type", "Well Depth (ft)", "Overburden (ft)", "well"],
                download_filename=f"aquifer_wells_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_wells",
            )

        use_lite = results.get("use_lite_popups", False)
        _render_map(samples_agg_df, aquifers_df, wells_df, boundaries, context, use_lite)

    else:
        st.info("Select parameters in the sidebar and click 'Execute Query' to run the analysis.")


def _render_map(samples_agg_df, aquifers_df, wells_df, boundaries, context, use_lite: bool = False) -> None:
    """Render the interactive 3-layer map: aquifer polygons, sample points, wells."""
    has_samples = not samples_agg_df.empty and "spWKT" in samples_agg_df.columns
    has_aquifers = not aquifers_df.empty and "aquiferwkt" in aquifers_df.columns
    has_wells = not wells_df.empty and "wellwkt" in wells_df.columns

    if not has_samples and not has_aquifers and not has_wells:
        return

    st.markdown("---")
    st.markdown("### Interactive Map")

    try:
        samplepts_gdf = create_geodataframe(samples_agg_df, "spWKT") if has_samples else None
        aquifers_gdf = create_geodataframe(aquifers_df, "aquiferwkt") if has_aquifers else None
        wells_gdf = create_geodataframe(wells_df, "wellwkt") if has_wells else None

        active_gdfs = [g for g in [samplepts_gdf, aquifers_gdf, wells_gdf] if g is not None]
        if not active_gdfs:
            st.warning("Could not parse geometry data for mapping.")
            return

        map_obj = create_base_map(gdf_list=active_gdfs, zoom=8)
        add_boundary_layers(map_obj, boundaries, context.region_code)

        if aquifers_gdf is not None and not aquifers_gdf.empty:
            sp = StripePattern(angle=-30, color=COLOR_AQUIFER, space_color='white', space_opacity=0.75)
            sp.add_to(map_obj)
            aquifers_gdf.explore(
                m=map_obj,
                color=COLOR_AQUIFER,
                style_kwds={"weight": 2.5, "style_function": lambda x: {"fillPattern": sp}},
                popup=["aquifer"],
                tooltip=False,
                name=f'<span style="color: {COLOR_AQUIFER};">Aquifers</span>',
                show=True,
            )

        if wells_gdf is not None and not wells_gdf.empty:
            fields = [c for c in ["welllabel", "Well Use", "Well Type", "Well Depth (ft)", "Overburden (ft)"] if c in wells_gdf.columns]
            add_point_layer(
                map_obj, wells_gdf,
                name=f'<span style="color:{COLOR_WELL};">Connected Wells</span>',
                color=COLOR_WELL, popup_fields=fields, radius=5,
            )

        if samplepts_gdf is not None and not samplepts_gdf.empty:
            popup_fields = SAMPLE_POPUP_FIELDS_LITE if use_lite else SAMPLE_POPUP_FIELDS
            popup_kwds = {"max_width": 500, "max_height": 400, "parse_html": True} if use_lite else SAMPLE_POPUP_KWDS
            add_sample_layer(
                map_obj, samplepts_gdf,
                popup_fields=popup_fields, popup_kwds=popup_kwds,
                name=f'<span style="color:{COLOR_SAMPLE};">Sample Points</span>',
                radius=7,
            )

        finalize_map(map_obj)
        import streamlit.components.v1 as components
        map_html = map_obj._repr_html_()
        components.html(map_html, height=600)
        render_map_legend([
            "**Striped areas** = Aquifers connected to sample points",
            "**Orange circles** = Sample points",
            "**Dark blue circles** = Potentially connected water wells",
            "**Boundary outline** = Selected region",
        ])

    except Exception as e:
        st.error(f"Error rendering map: {e}")
