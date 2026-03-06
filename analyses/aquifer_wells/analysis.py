"""
Aquifer-Connected Wells Analysis
Find PFAS-contaminated sample points, connected aquifers, and water wells
(Maine MGS) that may be at risk through shared aquifer connectivity.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from analysis_registry import AnalysisContext
from analyses.aquifer_wells.queries import (
    execute_aquifer_query,
    execute_sample_history_query,
)
from filters.substance import get_cached_substances_with_labels
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
    finalize_map, render_map_legend, render_folium_map,
)
from components.execute_button import render_execute_button
from components.analysis_state import AnalysisState, check_old_session_keys
from components.step_execution import StepExecutor
from components.query_debug import render_executed_queries
from components.eta_display import render_simple_eta
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
    - Finds PFAS-contaminated sample points in your selected region
    - Identifies aquifers spatially connected to those sample points
    - Finds water wells connected to those same aquifers that may be at risk

    **Process:** Find contaminated samples -> Identify connected aquifers -> Find at-risk wells

    **Use case:** Determine if water wells may be at risk through shared aquifer connectivity to PFAS contamination

   
    """)

    state = AnalysisState(context.analysis_key)
    state.init_if_missing("executed_queries", [])

    # === SIDEBAR PARAMETERS ===
    st.sidebar.markdown("### Query Parameters")

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
            if name not in substance_map or str(uri).endswith("_A"):
                substance_map[name] = uri

    selected_substance_display = st.sidebar.selectbox(
        "Select PFAS Substance (Optional)",
        ["-- All Substances --"] + sorted(substance_map.keys()),
        help="Filter to a specific PFAS compound, or leave as 'All Substances'",
    )

    selected_substance_uri = None
    selected_substance_name = None
    if selected_substance_display != "-- All Substances --":
        selected_substance_name = selected_substance_display
        selected_substance_uri = substance_map.get(selected_substance_display)

    st.sidebar.markdown("---")

    conc_filter = render_concentration_filter(context.analysis_key, default_max=500)

    execute_clicked = render_execute_button(
        help_text="Execute the aquifer-connected wells analysis"
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

        executor = StepExecutor(num_steps=1)
        combined_df = pd.DataFrame()
        executed_queries = []
        step_eta_by_label = {s.label: s for s in run_eta.step_estimates}

        with executor.step(1, "Finding samples, aquifers & connected wells...") as step:
            combined_df, error, debug = execute_aquifer_query(
                region_code=context.region_code,
                substance_uri=selected_substance_uri,
                min_conc=min_conc,
                max_conc=max_conc,
                include_nondetects=include_nondetects,
            )
            step_info = build_query_debug_entry(
                "Step 1: Samples, Aquifers & Wells", debug,
                row_count=len(combined_df), error=error,
            )
            executed_queries.append(step_info)
            if error:
                step.error(f"Query failed: {error}")
            elif not combined_df.empty:
                n_sp = combined_df["sp"].nunique() if "sp" in combined_df.columns else 0
                n_aq = combined_df["aquifer"].nunique() if "aquifer" in combined_df.columns else 0
                n_wl = combined_df["well"].nunique() if "well" in combined_df.columns else 0
                step.success(f"Found {n_sp} sample points, {n_aq} aquifer(s), {n_wl} well(s)")
            else:
                step.warning("No results found for the selected parameters")

        # Split combined result into thematic DataFrames
        samplepts_df = pd.DataFrame()
        aquifers_df = pd.DataFrame()
        wells_df = pd.DataFrame()
        if not combined_df.empty:
            if "sp" in combined_df.columns and "spwkt" in combined_df.columns:
                samplepts_df = (
                    combined_df.groupby(["sp", "spwkt"], as_index=False)
                    .agg(max_conc=("numericValue", "max"))
                )
            if "aquifer" in combined_df.columns and "aquiferwkt" in combined_df.columns:
                aquifers_df = (
                    combined_df[["aquifer", "aquiferwkt"]]
                    .drop_duplicates(subset=["aquifer"])
                    .reset_index(drop=True)
                )
            well_cols = ["well", "welllabel", "welluse", "welltype", "welldepthft", "welloverburdenft", "wellwkt"]
            available_well_cols = [c for c in well_cols if c in combined_df.columns]
            if available_well_cols and "well" in combined_df.columns:
                wells_df = (
                    combined_df[available_well_cols]
                    .drop_duplicates(subset=["well"])
                    .reset_index(drop=True)
                )

        boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)

        record_executed_query_batch(
            request=run_request,
            executed_queries=executed_queries,
            step_eta_by_label=step_eta_by_label,
        )
        state.set("executed_queries", executed_queries)
        state.set_results({
            "samplepts_df": samplepts_df,
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
        samplepts_df = results.get("samplepts_df", pd.DataFrame())
        aquifers_df = results.get("aquifers_df", pd.DataFrame())
        wells_df = results.get("wells_df", pd.DataFrame())
        boundaries = results.get("boundaries", {})
        params_data = results.get("params_data", [])
        query_region_code = results.get("query_region_code")

        st.markdown("---")
        render_parameter_table(params_data)
        st.markdown("### Query Results")
        st.markdown("---")

        if not samplepts_df.empty:
            metrics = [{"label": "Contaminated Sample Points", "value": len(samplepts_df)}]
            if "max_conc" in samplepts_df.columns:
                vals = pd.to_numeric(samplepts_df["max_conc"], errors="coerce")
                if vals.notna().any():
                    metrics.append({"label": "Max Concentration", "value": f"{vals.max():.2f} ng/L"})
            render_step_results(
                "Contaminated Sample Points", samplepts_df, metrics,
                "View Sample Points Data",
                display_columns=["sp", "max_conc"],
                download_filename=f"aquifer_samplepts_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_samplepts",
                show_stats=True, stats_column="max_conc",
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
                display_columns=["welllabel", "welluse", "welltype", "welldepthft", "welloverburdenft", "well"],
                download_filename=f"aquifer_wells_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_wells",
            )

        _render_map(samplepts_df, aquifers_df, wells_df, boundaries, context)
        _render_drill_down(samplepts_df, state, context)

    else:
        st.info("Select parameters in the sidebar and click 'Execute Query' to run the analysis.")


def _render_map(samplepts_df, aquifers_df, wells_df, boundaries, context) -> None:
    """Render the interactive 3-layer map: aquifer polygons, sample points, wells."""
    has_samples = not samplepts_df.empty and "spwkt" in samplepts_df.columns
    has_aquifers = not aquifers_df.empty and "aquiferwkt" in aquifers_df.columns
    has_wells = not wells_df.empty and "wellwkt" in wells_df.columns

    if not has_samples and not has_aquifers and not has_wells:
        return

    st.markdown("---")
    st.markdown("### Interactive Map")

    try:
        samplepts_gdf = create_geodataframe(samplepts_df, "spwkt") if has_samples else None
        aquifers_gdf = create_geodataframe(aquifers_df, "aquiferwkt") if has_aquifers else None
        wells_gdf = create_geodataframe(wells_df, "wellwkt") if has_wells else None
        # Well coordinates are stored as (lat, lon) instead of (lon, lat) — swap them
        if wells_gdf is not None and not wells_gdf.empty:
            from shapely.ops import transform
            wells_gdf["geometry"] = wells_gdf["geometry"].apply(
                lambda geom: transform(lambda x, y: (y, x), geom)
            )

        active_gdfs = [g for g in [samplepts_gdf, aquifers_gdf, wells_gdf] if g is not None]
        if not active_gdfs:
            st.warning("Could not parse geometry data for mapping.")
            return

        map_obj = create_base_map(gdf_list=active_gdfs, zoom=8)
        add_boundary_layers(map_obj, boundaries, context.region_code)

        if aquifers_gdf is not None and not aquifers_gdf.empty:
            popup_cols = [c for c in ["aquifer"] if c in aquifers_gdf.columns]
            aquifers_gdf.explore(
                m=map_obj,
                color="RoyalBlue",
                style_kwds={"fillOpacity": 0.25, "weight": 2, "color": "RoyalBlue"},
                popup=popup_cols or True,
                tooltip=False,
                name='<span style="color:RoyalBlue;">Aquifers</span>',
                show=True,
            )

        if wells_gdf is not None and not wells_gdf.empty:
            fields = [c for c in ["welllabel", "welluse", "welltype", "welldepthft", "welloverburdenft"] if c in wells_gdf.columns]
            add_point_layer(
                map_obj, wells_gdf,
                name='<span style="color:DeepSkyBlue;">Connected Wells</span>',
                color="DeepSkyBlue", popup_fields=fields, radius=5,
            )

        if samplepts_gdf is not None and not samplepts_gdf.empty:
            fields = [c for c in ["sp", "max_conc"] if c in samplepts_gdf.columns]
            add_point_layer(
                map_obj, samplepts_gdf,
                name='<span style="color:DarkOrange;">PFAS Sample Points</span>',
                color="DarkOrange", popup_fields=fields, radius=7,
            )

        finalize_map(map_obj)
        render_folium_map(map_obj)
        render_map_legend([
            "**Blue polygons** = Aquifers connected to contaminated sample points",
            "**Orange circles** = PFAS contaminated sample points",
            "**Light blue circles** = Potentially connected water wells",
            "**Boundary outline** = Selected region",
        ])

    except Exception as e:
        st.error(f"Error rendering map: {e}")


def _render_drill_down(samplepts_df: pd.DataFrame, state: AnalysisState, context: AnalysisContext) -> None:
    """Render the sample point history drill-down section."""
    if samplepts_df.empty or "sp" not in samplepts_df.columns:
        return

    st.markdown("---")
    st.markdown("### Sample Point History")
    st.caption("Select a sample point to view its full PFAS measurement history.")

    sp_uris = samplepts_df["sp"].dropna().unique().tolist()
    short_labels = {_short_uri(u): u for u in sp_uris}

    selected_label = st.selectbox(
        "Sample point",
        ["-- Select --"] + list(short_labels.keys()),
        key=f"{context.analysis_key}_history_sp",
    )

    if not selected_label or selected_label == "-- Select --":
        return

    selected_uri = short_labels[selected_label]
    cached_uri = state.get("history_sp", None)

    if cached_uri != selected_uri:
        with st.spinner("Loading measurement history..."):
            history_df, hist_error, _ = execute_sample_history_query(selected_uri)
        state.set("history_sp", selected_uri)
        state.set("history_df", history_df)
        state.set("history_error", hist_error)
    else:
        history_df = state.get("history_df", pd.DataFrame())
        hist_error = state.get("history_error", None)

    if hist_error:
        st.error(f"Error loading history: {hist_error}")
        return

    if history_df.empty:
        st.info("No measurement history found for this sample point.")
        return

    st.markdown(f"**Measurements for:** `{selected_label}`")
    st.dataframe(history_df, use_container_width=True, hide_index=True)

    csv = history_df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"history_{_short_uri(selected_uri)}.csv",
        mime="text/csv",
        key=f"{context.analysis_key}_history_download",
    )


def _short_uri(uri: str) -> str:
    """Extract a short readable label from a URI."""
    if not uri:
        return uri
    return str(uri).rstrip("/").rsplit("/", 1)[-1].rsplit("#", 1)[-1]
