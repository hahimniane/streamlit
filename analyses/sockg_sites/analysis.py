"""
SOCKG Sites & Facilities Analysis
Optional state filter; shows SOCKG locations and nearby facilities.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from analysis_registry import AnalysisContext
from analyses.sockg_sites.queries import get_sockg_locations, get_sockg_facilities
from filters.region import get_region_boundary, add_region_boundary_layers

# Shared components
from core.geometry import create_geodataframe, convert_to_centroids
from components.parameter_display import render_parameter_table
from components.result_display import render_step_results
from components.map_rendering import (
    OTHER_FACILITY_MARKER_RADIUS,
    PFAS_FACILITY_MARKER_RADIUS,
    create_base_map,
    add_point_layer,
    finalize_map,
    render_map_legend,
    render_folium_map,
)
from components.execute_button import render_execute_button
from components.analysis_state import AnalysisState
from components.step_execution import StepExecutor
from components.query_debug import render_executed_queries
from components.eta_display import render_simple_eta
from core.runtime_eta import (
    build_eta_request,
    estimate_eta,
    record_executed_query_batch,
)


def main(context: AnalysisContext) -> None:
    """Render the SOCKG analysis UI."""
    st.markdown("""
    **What this analysis does:**
    - Retrieves SOCKG locations (ARS sites)
    - Finds nearby facilities and flags PFAS-related industries

    **2-Step Process:** Retrieve SOCKG locations -> Find nearby facilities

    **State filter:** Optional (use the region selector in the sidebar)
    """)

    state = AnalysisState(context.analysis_key)
    state.init_if_missing("executed_queries", [])
    state_code = context.selected_state_code
    state_display = context.selected_state_name or "All states"

    # Sidebar
    st.sidebar.markdown("### Query Parameters")
    st.sidebar.caption(f"State filter: {state_display}")

    execute_clicked = render_execute_button(
        key=f"{context.analysis_key}_execute",
        help_text="Execute the SOCKG analysis"
    )

    preview_request = build_eta_request(
        analysis_key=context.analysis_key,
        region_code=context.region_code,
        state_code=context.selected_state_code,
        min_conc=0,
        max_conc=0,
        include_nondetects=False,
        has_substance_filter=False,
        has_material_filter=False,
    )
    render_simple_eta(estimate_eta(preview_request))

    # === QUERY EXECUTION ===
    if execute_clicked:
        st.markdown("---")
        st.subheader("Query Execution")

        run_request = build_eta_request(
            analysis_key=context.analysis_key,
            region_code=context.region_code,
            state_code=context.selected_state_code,
            min_conc=0,
            max_conc=0,
            include_nondetects=False,
            has_substance_filter=False,
            has_material_filter=False,
        )
        run_eta = estimate_eta(run_request)

        executor = StepExecutor(num_steps=2)
        sites_df = pd.DataFrame()
        facilities_df = pd.DataFrame()
        executed_queries = []
        step_eta_by_label = {s.label: s for s in run_eta.step_estimates}

        with executor.step(1, "Retrieving SOCKG locations...") as step:
            sites_df, sites_debug = get_sockg_locations(state_code)
            executed_queries.append(sites_debug)
            if not sites_df.empty:
                step.success(f"Step 1: Found {len(sites_df)} locations")
            else:
                step.info("Step 1: No SOCKG locations found")

        with executor.step(2, "Finding nearby facilities...") as step:
            facilities_df, facilities_debug = get_sockg_facilities(state_code)
            executed_queries.append(facilities_debug)
            if not facilities_df.empty:
                step.success(f"Step 2: Found {len(facilities_df)} facilities")
            else:
                step.info("Step 2: No facilities found")

        record_executed_query_batch(
            request=run_request,
            executed_queries=executed_queries,
            step_eta_by_label=step_eta_by_label,
        )
        state.set("executed_queries", executed_queries)
        state.set_results({
            "sites_df": sites_df, "facilities_df": facilities_df,
            "state_display": state_display, "state_code": state_code,
            "region_boundary_df": get_region_boundary(state_code) if state_code else None,
            "params_data": [{"Parameter": "State filter", "Value": state_display}],
            "executed_queries": executed_queries,
        })

    render_executed_queries(state.get("executed_queries", []))

    # === DISPLAY RESULTS ===
    if not state.has_results:
        st.info("Click 'Execute Query' to run the analysis. State filter is optional.")
        return

    results = state.get_results()
    sites_df = results.get("sites_df", pd.DataFrame())
    facilities_df = results.get("facilities_df", pd.DataFrame())
    state_code = results.get("state_code")
    region_boundary_df = results.get("region_boundary_df")
    params_data = results.get("params_data", [])

    st.markdown("---")
    render_parameter_table(params_data)
    st.markdown("### Query Results")

    pfas_count = 0
    if not facilities_df.empty and "PFASusing" in facilities_df.columns:
        pfas_count = facilities_df["PFASusing"].astype(str).str.lower().eq("true").sum()

    if sites_df.empty and facilities_df.empty:
        st.warning("No results found. Try again or remove the state filter.")
        return

    st.markdown("---")

    # Step 1: SOCKG Locations
    if not sites_df.empty:
        render_step_results("Step 1: SOCKG Locations", sites_df, [{"label": "Total Locations", "value": len(sites_df)}],
            "View SOCKG Locations Data",
            display_columns=["locationId", "locationDescription", "location"],
            download_filename=f"sockg_locations_{state_code or 'all'}.csv",
            download_key=f"download_{context.analysis_key}_locations",
        )

    # Step 2: Facilities
    if not facilities_df.empty:
        render_step_results("Step 2: Facilities", facilities_df, [
                {"label": "Total Facilities", "value": len(facilities_df)},
                {"label": "PFAS-Related Facilities", "value": pfas_count},
            ],
            "View Facilities Data",
            display_columns=["facilityName", "industrySector", "industrySubsector", "PFASusing", "industries", "locations"],
            download_filename=f"sockg_facilities_{state_code or 'all'}.csv",
            download_key=f"download_{context.analysis_key}_facilities",
        )

    # Map
    _render_map(sites_df, facilities_df, region_boundary_df, state_code)


def _render_map(sites_df, facilities_df, region_boundary_df, state_code) -> None:
    """Render the interactive map."""
    sites_gdf = create_geodataframe(sites_df, 'locationGeometry') if not sites_df.empty else None
    facilities_gdf = None

    if not facilities_df.empty and "facWKT" in facilities_df.columns:
        fac_with_wkt = facilities_df[facilities_df["facWKT"].notna()].copy()
        if not fac_with_wkt.empty:
            fac_with_wkt["PFASusing"] = fac_with_wkt["PFASusing"].astype(str).str.lower() == "true"
            facilities_gdf = create_geodataframe(fac_with_wkt, 'facWKT')

    if sites_gdf is None and facilities_gdf is None:
        return

    st.markdown("---")
    st.markdown("### Interactive Map")

    map_obj = create_base_map(gdf_list=[sites_gdf, facilities_gdf], zoom=6)

    tooltip_style = (
        "background-color: white; border-radius: 3px; box-shadow: 3px 3px 5px grey; "
        "padding: 10px; font-family: sans-serif; font-size: 14px; max-width: 450px; overflow-wrap: break-word;"
    )

    # Add SOCKG sites
    if sites_gdf is not None and not sites_gdf.empty:
        sites_points = convert_to_centroids(sites_gdf)
        site_fields = [c for c in ["locationId", "locationDescription", "location"] if c in sites_points.columns]
        add_point_layer(map_obj, sites_points,
            name='<span style="color:Red;">SOCKG Locations</span>', color='Red',
            popup_fields=site_fields, radius=6,
            tooltip_kwds=dict(aliases=site_fields, localize=True, labels=True, sticky=False, style=tooltip_style),
            popup_kwds=dict(aliases=site_fields, localize=True, labels=True, style=tooltip_style))

    # Add facilities (split by PFAS status)
    if facilities_gdf is not None and not facilities_gdf.empty:
        facilities_points = convert_to_centroids(facilities_gdf)
        pfas_facilities = facilities_points[facilities_points["PFASusing"]]
        other_facilities = facilities_points[~facilities_points["PFASusing"]]

        facility_fields = [c for c in ["facilityName", "industrySector", "industrySubsector", "industries", "locations"] if c in facilities_points.columns]
        pfas_fields = [c for c in ["facilityName", "industrySector", "industrySubsector", "PFASusing", "industries", "locations"] if c in pfas_facilities.columns]

        tooltip_style_wide = tooltip_style.replace("450px", "650px")

        if not other_facilities.empty:
            add_point_layer(map_obj, other_facilities,
                name='<span style="color:MidnightBlue;">Other Facilities</span>', color='MidnightBlue',
                popup_fields=facility_fields, radius=OTHER_FACILITY_MARKER_RADIUS,
                tooltip_kwds=dict(aliases=facility_fields, localize=True, labels=True, sticky=False, style=tooltip_style_wide),
                popup_kwds=dict(aliases=facility_fields, localize=True, labels=True, style=tooltip_style_wide))

        if not pfas_facilities.empty:
            add_point_layer(map_obj, pfas_facilities,
                name='<span style="color:DarkRed;">PFAS-Related Facilities</span>', color='DarkRed',
                popup_fields=pfas_fields, radius=PFAS_FACILITY_MARKER_RADIUS,
                tooltip_kwds=dict(aliases=pfas_fields, localize=True, labels=True, sticky=False, style=tooltip_style_wide),
                popup_kwds=dict(aliases=pfas_fields, localize=True, labels=True, style=tooltip_style_wide))

    add_region_boundary_layers(map_obj, region_boundary_df=region_boundary_df, region_code=state_code)
    finalize_map(map_obj)
    render_folium_map(map_obj)
    render_map_legend([
        "**Red circles** = SOCKG locations (ARS sites)",
        "**Dark blue circles** = Other facilities",
        "**Dark red circles** = PFAS-related facilities"
    ])
