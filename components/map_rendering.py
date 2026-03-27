"""
Shared map rendering utilities.
Consolidates repeated map creation, styling, and layer management across analyses.
"""
from __future__ import annotations

import re
from typing import Optional, List, Dict, Any, Callable
import folium
import geopandas as gpd
import pandas as pd

from core.geometry import get_map_center
from filters.region import add_region_boundary_layers


# Default popup CSS applied to all maps
POPUP_CSS = """
<style>
.leaflet-popup { max-width: 90vw !important; }
.leaflet-popup-content-wrapper { max-width: 90vw !important; }
.leaflet-popup-content {
  min-width: 300px !important;
  max-width: 80vw !important;
  width: auto !important;
  max-height: 500px;
  overflow-y: auto;
  overflow-x: auto;
}
.leaflet-popup-content table {
  width: max-content !important;
  table-layout: auto;
}
.leaflet-popup-content td {
  overflow-wrap: anywhere;
  white-space: normal !important;
}
.leaflet-popup-content th {
  white-space: nowrap !important;
  overflow-wrap: normal;
}
.leaflet-popup-content a, .leaflet-tooltip a {
  display: inline-block;
  max-width: 100%;
  overflow-wrap: anywhere;
  white-space: normal !important;
}
</style>
"""

# ---------------------------------------------------------------------------
# Design-system color palette (ColorBrewer2)
# See: https://github.com/SAWGraph/explorer-app/wiki/Design-System-References
# ---------------------------------------------------------------------------

# Water features — PuBu sequential
COLOR_WATERSHED = "#d0d1e6"
COLOR_AQUIFER = "#74a9cf"
COLOR_FLOWLINE = "#2b8cbe"
COLOR_WELL = "#045a8d"

# Sample points — PuOr diverging palette (purple=low, orange=high concentration)
COLOR_SAMPLE = "#fdb863"
SAMPLE_PUOR_PALETTE = [
    "#542788",  # purple extreme  — non-detect / zero
    "#8073ac",  #                 — 0 < c ≤ 4 ng/L
    "#b2abd2",  #                 — 4 < c ≤ 20
    "#d8daeb",  #                 — 20 < c ≤ 50
    "#fee0b6",  # neutral center  — 50 < c ≤ 100
    "#fdb863",  #                 — 100 < c ≤ 200
    "#e08214",  #                 — 200 < c ≤ 400
    "#b35806",  # orange extreme  — > 400
]
SAMPLE_CONC_BREAKS = [0, 4, 20, 50, 100, 200, 400]

# Facilities — 9-class Reds (primary), 9-class Purples (secondary)
# Full palettes kept for reference; the _VISIBLE slices skip the lightest
# shades that are invisible as map markers and layer-control text.
_FACILITY_COLORS_REDS_FULL = [
    "#fff5f0", "#fee0d2", "#fcbba1", "#fc9272", "#fb6a4a",
    "#ef3b2c", "#cb181d", "#a50f15", "#67000d",
]
_FACILITY_COLORS_PURPLES_FULL = [
    "#fcfbfd", "#efedf5", "#dadaeb", "#bcbddc", "#9e9ac8",
    "#807dba", "#6a51a3", "#54278f", "#3f007d",
]

# Visible subsets — drop the 3 lightest shades so every layer is legible
FACILITY_COLORS_REDS = _FACILITY_COLORS_REDS_FULL[3:]      # #fc9272 … #67000d  (6 colors)
FACILITY_COLORS_PURPLES = _FACILITY_COLORS_PURPLES_FULL[3:]  # #bcbddc … #3f007d  (6 colors)

# Default color palette for grouped facility/industry layers (visible Reds)
LAYER_COLORS = FACILITY_COLORS_REDS

# Shared marker sizing defaults.
# Update these values to tune marker sizes app-wide.
DEFAULT_POINT_RADIUS = 6
FACILITY_MARKER_RADIUS = 8
OTHER_FACILITY_MARKER_RADIUS = 6
PFAS_FACILITY_MARKER_RADIUS = 7


def extract_frs_registry_id(facility_uri: Any) -> str:
    """Extract the FRS registry id suffix from a facility URI/value."""
    value = str(facility_uri or "").strip()
    if not value:
        return ""
    if "." in value:
        return value.split(".")[-1]
    if "#" in value:
        return value.split("#")[-1]
    if "/" in value:
        return value.rsplit("/", 1)[-1]
    return value


def add_facility_link_column(
    df: pd.DataFrame,
    source_col: str = "facility",
    target_col: str = "Facility ID",
) -> pd.DataFrame:
    """
    Add a clickable link column using the facility URI directly.
    """
    if df is None or df.empty or source_col not in df.columns:
        return df

    def _link_for(value: Any) -> Any:
        uri = str(value).strip() if value else ""
        if not uri or uri == "nan":
            return value
        label = uri.rsplit("/", 1)[-1].rsplit("#", 1)[-1]
        return f'<a href="{uri}" target="_blank">{label}</a>'

    result = df.copy()
    result[target_col] = result[source_col].apply(_link_for)
    return result


def add_short_code_column(
    df: pd.DataFrame,
    source_col: str,
    target_col: str,
    delimiter: str = "#",
) -> pd.DataFrame:
    """Add a short-code helper column by splitting URI-like values."""
    if df is None or df.empty or source_col not in df.columns:
        return df

    result = df.copy()
    result[target_col] = result[source_col].apply(
        lambda x: str(x).split(delimiter)[-1] if x else x
    )
    return result


def extract_naics_code(uri: Any) -> str:
    """Extract the numeric NAICS code from a URI like http://w3id.org/fio/v1/naics#NAICS-321113."""
    value = str(uri or "").strip()
    match = re.search(r"(\d+)$", value)
    return match.group(1) if match else ""


def add_naics_link_column(
    df: pd.DataFrame,
    source_col: str = "industryCode",
    target_col: str = "NAICS Code",
) -> pd.DataFrame:
    """
    Add an HTML hyperlink column for NAICS codes (for use in map popups).
    Displays the numeric code as a clickable link to the NAICS website.
    """
    if df is None or df.empty or source_col not in df.columns:
        return df

    def _link(value: Any) -> Any:
        code = extract_naics_code(value)
        if not code:
            return value
        return (
            f'<a href="https://www.naics.com/naics-code-description/?code={code}"'
            f' target="_blank">{code}</a>'
        )

    result = df.copy()
    result[target_col] = result[source_col].apply(_link)
    return result


def add_naics_url_column(
    df: pd.DataFrame,
    source_col: str = "industryCode",
    target_col: str = "industryCode_url",
) -> pd.DataFrame:
    """
    Add a plain URL column for NAICS codes (for use with st.column_config.LinkColumn in tables).
    """
    if df is None or df.empty or source_col not in df.columns:
        return df

    def _url(value: Any) -> Any:
        code = extract_naics_code(value)
        if not code:
            return None
        return f"https://www.naics.com/naics-code-description/?code={code}"

    result = df.copy()
    result[target_col] = result[source_col].apply(_url)
    return result


def _concentration_to_color(value) -> str:
    """Map a concentration value (ng/L) to a PuOr palette color."""
    if value is None:
        return SAMPLE_PUOR_PALETTE[0]
    try:
        v = float(value)
    except (ValueError, TypeError):
        return SAMPLE_PUOR_PALETTE[0]
    if v <= 0:
        return SAMPLE_PUOR_PALETTE[0]
    for i, brk in enumerate(SAMPLE_CONC_BREAKS):
        if v <= brk:
            return SAMPLE_PUOR_PALETTE[i]
    return SAMPLE_PUOR_PALETTE[-1]


def sample_point_style(feature: Dict[str, Any]) -> Dict[str, Any]:
    """Shared marker style for sample points using the PuOr concentration palette.

    Reads ``overall_max_result`` from feature properties to determine both
    fill color (PuOr palette) and radius (scaled by concentration).
    """
    props = (feature or {}).get("properties", {}) or {}
    max_val = props.get("overall_max_result")

    fill_color = _concentration_to_color(max_val)

    radius = 4
    if max_val is not None:
        try:
            v = float(max_val)
            if v > 0:
                radius = 4 if v < 40 else (v / 16 if v < 160 else 12)
        except (ValueError, TypeError):
            pass

    return {
        "radius": max(3, min(12, radius)),
        "fillColor": fill_color,
        "color": "DimGray",
        "fillOpacity": 0.7,
        "opacity": 0.3,
    }


def add_sample_layer(
    map_obj: folium.Map,
    gdf: gpd.GeoDataFrame,
    popup_fields: List[str],
    popup_kwds: Dict = None,
    name: str = None,
    radius: int = DEFAULT_POINT_RADIUS,
) -> None:
    """Add a sample-point layer colored by PuOr concentration palette.

    This is the canonical way to render PFAS sample points on any analysis map.
    Uses ``sample_point_style`` for per-feature color and size.
    """
    if gdf is None or gdf.empty:
        return

    label = name or f'<span style="color:{COLOR_SAMPLE};">Samples</span>'
    gdf.explore(
        m=map_obj,
        name=label,
        color=COLOR_SAMPLE,
        marker_kwds=dict(radius=radius),
        marker_type="circle_marker",
        popup=popup_fields,
        popup_kwds=popup_kwds or {},
        style_kwds=dict(style_function=sample_point_style),
    )


def create_base_map(
    gdf_list: List[Optional[gpd.GeoDataFrame]] = None,
    center: tuple = None,
    zoom: int = 8,
    apply_popup_css: bool = True
) -> folium.Map:
    """
    Create a base Folium map centered on the provided data or default location.

    Args:
        gdf_list: List of GeoDataFrames to use for centering (in priority order)
        center: Override center coordinates (lat, lon)
        zoom: Initial zoom level
        apply_popup_css: Whether to apply popup styling CSS

    Returns:
        Configured Folium Map object
    """
    if center:
        map_center = center
    elif gdf_list:
        map_center = get_map_center(gdf_list)
    else:
        map_center = (39.8, -98.5)  # Default: center of US

    map_obj = folium.Map(location=list(map_center), zoom_start=zoom)

    if apply_popup_css:
        try:
            map_obj.get_root().header.add_child(folium.Element(POPUP_CSS))
        except Exception:
            pass

    return map_obj


def add_boundary_layers(
    map_obj: folium.Map,
    boundaries: Dict[str, Optional[pd.DataFrame]],
    region_code: Optional[str] = None,
    warn_fn: Callable = None
) -> None:
    """
    Add region boundary layers to the map.

    Args:
        map_obj: Folium map to add layers to
        boundaries: Dict from fetch_boundaries() with 'state', 'county', 'region' keys
        region_code: Region code for styling
        warn_fn: Optional warning function (e.g., st.warning)
    """
    add_region_boundary_layers(
        map_obj,
        state_boundary_df=boundaries.get('state'),
        county_boundary_df=boundaries.get('county'),
        region_boundary_df=boundaries.get('region'),
        region_code=region_code,
        warn_fn=warn_fn
    )


def add_point_layer(
    map_obj: folium.Map,
    gdf: gpd.GeoDataFrame,
    name: str,
    color: str,
    popup_fields: List[str] = None,
    tooltip_fields: List[str] = None,
    radius: int = DEFAULT_POINT_RADIUS,
    show: bool = True,
    style_function: Callable = None,
    marker_type: str = 'circle_marker',
    popup_kwds: Dict = None,
    tooltip_kwds: Dict = None
) -> None:
    """
    Add a point layer to the map with consistent styling.

    Args:
        map_obj: Folium map to add layer to
        gdf: GeoDataFrame with point geometries
        name: Layer name (supports HTML for colored labels)
        color: Marker color
        popup_fields: Fields to show in popup (None = show all)
        tooltip_fields: Fields to show in tooltip (None = same as popup)
        radius: Marker radius
        show: Whether layer is visible by default
        style_function: Optional style function for markers
        marker_type: Type of marker ('circle_marker', 'marker')
        popup_kwds: Additional popup keyword arguments
        tooltip_kwds: Additional tooltip keyword arguments
    """
    if gdf is None or gdf.empty:
        return

    # Use popup fields as tooltip if not specified
    if tooltip_fields is None:
        tooltip_fields = popup_fields

    # Build explore kwargs
    explore_kwargs = {
        'm': map_obj,
        'name': name,
        'color': color,
        'marker_kwds': {'radius': radius},
        'marker_type': marker_type,
        'popup': popup_fields if popup_fields else True,
        'tooltip': tooltip_fields if tooltip_fields else None,
        'show': show
    }

    if popup_kwds:
        explore_kwargs['popup_kwds'] = popup_kwds
    if tooltip_kwds:
        explore_kwargs['tooltip_kwds'] = tooltip_kwds

    # Add style function if provided
    if style_function:
        explore_kwargs['style_kwds'] = {'style_function': style_function}

    gdf.explore(**explore_kwargs)


def add_line_layer(
    map_obj: folium.Map,
    gdf: gpd.GeoDataFrame,
    name: str,
    color: str,
    weight: int = 3,
    opacity: float = 0.5,
    popup_fields: List[str] = None,
    show: bool = True
) -> None:
    """
    Add a line layer (e.g., flowlines, streams) to the map.

    Args:
        map_obj: Folium map to add layer to
        gdf: GeoDataFrame with line geometries
        name: Layer name
        color: Line color
        weight: Line weight
        opacity: Line opacity
        popup_fields: Fields to show in popup
        show: Whether layer is visible by default
    """
    if gdf is None or gdf.empty:
        return

    gdf.explore(
        m=map_obj,
        name=name,
        color=color,
        style_kwds={'weight': weight, 'opacity': opacity},
        popup=popup_fields if popup_fields else False,
        tooltip=False,
        show=show
    )


def add_grouped_point_layers(
    map_obj: folium.Map,
    gdf: gpd.GeoDataFrame,
    group_column: str,
    popup_fields: List[str] = None,
    colors: List[str] = None,
    radius: int = FACILITY_MARKER_RADIUS,
    name_template: str = "{group} ({count})",
    popup_kwds: Dict = None,
    tooltip_kwds: Dict = None,
) -> None:
    """
    Add multiple point layers, one for each unique value in a grouping column.
    Each layer name is tagged with class="facility-layer" so the bulk toggle
    button (added via add_facility_toggle_button) can find them all at once.

    Args:
        map_obj: Folium map to add layers to
        gdf: GeoDataFrame with point geometries
        group_column: Column to group by (e.g., 'industryName')
        popup_fields: Fields to show in popup
        colors: List of colors to cycle through
        radius: Marker radius
        name_template: Template for layer names (uses {group} and {count})
        popup_kwds: Additional popup keyword arguments
        tooltip_kwds: Additional tooltip keyword arguments
    """
    if gdf is None or gdf.empty:
        return

    if group_column not in gdf.columns or not gdf[group_column].notna().any():
        # Fallback: add as single layer
        add_point_layer(
            map_obj, gdf, "Facilities", "Purple",
            popup_fields=popup_fields, radius=radius, popup_kwds=popup_kwds,
            tooltip_kwds=tooltip_kwds,
        )
        return

    colors = colors or LAYER_COLORS
    unique_groups = sorted(gdf[group_column].dropna().unique())

    for idx, group in enumerate(unique_groups):
        group_gdf = gdf[gdf[group_column] == group]
        color = colors[idx % len(colors)]
        count = len(group_gdf)

        layer_name = name_template.format(group=group, count=count)
        colored_name = f'<span style="color:{color};">{layer_name}</span>'

        add_point_layer(
            map_obj, group_gdf, colored_name, color,
            popup_fields=popup_fields, radius=radius, popup_kwds=popup_kwds,
            tooltip_kwds=tooltip_kwds,
        )


def finalize_map(map_obj: folium.Map, collapsed: bool = True) -> None:
    """
    Finalize the map by adding layer control.

    Args:
        map_obj: Folium map to finalize
        collapsed: Whether layer control should be collapsed by default
    """
    folium.LayerControl(collapsed=collapsed).add_to(map_obj)


def render_map_legend(legend_items: List[str]) -> None:
    """
    Render a map legend as an info box using streamlit.

    Args:
        legend_items: List of legend description strings
    """
    import streamlit as st

    legend_text = "**Map Legend:**\n" + "\n".join(f"- {item}" for item in legend_items)
    st.info(legend_text)


def render_folium_map(map_obj, height: int = 1000) -> None:
    """
    Render a folium map with height proportional to its rendered width (16:9).
    Uses a JS ResizeObserver injected via components.html so it works in both
    normal and wide/full-screen modes even after st_folium sets height via JS.
    """
    import streamlit.components.v1 as components
    from streamlit_folium import st_folium

    st_folium(map_obj, width=None, height=height, returned_objects=[])

    # Inject 0-height iframe with JS that finds the map iframe (height > 100px)
    # and resizes it to maintain 16:9, updating on every window resize.
    components.html(
        """
        <script>
        (function () {
            try {
                var doc = window.parent.document;
                function resizeMaps() {
                    doc.querySelectorAll(
                        '[data-testid="stCustomComponentV1"] iframe'
                    ).forEach(function (f) {
                        if (f.offsetHeight > 100) {
                            var w = f.getBoundingClientRect().width;
                            if (w > 100) f.style.height = Math.round(w * 9 / 16) + 'px';
                        }
                    });
                }
                // Run shortly after map renders, then again to catch late paint
                setTimeout(resizeMaps, 150);
                setTimeout(resizeMaps, 600);
                // Re-run whenever the window is resized (sidebar toggle, fullscreen, etc.)
                doc.defaultView.addEventListener('resize', function () {
                    setTimeout(resizeMaps, 80);
                });
            } catch (e) {}
        })();
        </script>
        """,
        height=0,
    )
