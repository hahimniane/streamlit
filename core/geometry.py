"""
Geometry utilities for creating GeoDataFrames from WKT columns.
Consolidates repeated geometry parsing logic across analyses.
"""
from __future__ import annotations

from typing import Optional, List
import pandas as pd
import geopandas as gpd
from shapely import wkt


def create_geodataframe(
    df: pd.DataFrame,
    wkt_column: str,
    crs: str = "EPSG:4326"
) -> Optional[gpd.GeoDataFrame]:
    """
    Safely create a GeoDataFrame from a DataFrame with a WKT geometry column.

    Args:
        df: Source DataFrame
        wkt_column: Name of the column containing WKT geometry strings
        crs: Coordinate reference system (default: EPSG:4326)

    Returns:
        GeoDataFrame with parsed geometries, or None if parsing fails or no valid geometries
    """
    if df is None or df.empty:
        return None

    if wkt_column not in df.columns:
        return None

    # Filter to rows with valid WKT
    with_wkt = df[df[wkt_column].notna()].copy()
    if with_wkt.empty:
        return None

    try:
        with_wkt['geometry'] = with_wkt[wkt_column].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(with_wkt, geometry='geometry')
        gdf.set_crs(crs, inplace=True, allow_override=True)
        return gdf
    except Exception:
        return None


def get_map_center(
    gdf_list: List[Optional[gpd.GeoDataFrame]],
    default_center: tuple = (39.8, -98.5)
) -> tuple:
    """
    Calculate the center point for a map from a list of GeoDataFrames.
    Uses the first non-empty GeoDataFrame's centroid.

    Args:
        gdf_list: List of GeoDataFrames to check (in priority order)
        default_center: Default center if no valid geometries (lat, lon)

    Returns:
        Tuple of (latitude, longitude)
    """
    for gdf in gdf_list:
        if gdf is not None and not gdf.empty:
            try:
                centroids = gdf.geometry.centroid
                center_lat = centroids.y.mean()
                center_lon = centroids.x.mean()
                if pd.notna(center_lat) and pd.notna(center_lon):
                    return (center_lat, center_lon)
            except Exception:
                continue

    return default_center


def simplify_geometries(
    gdf: gpd.GeoDataFrame,
    tolerance: float = 0.001,
) -> gpd.GeoDataFrame:
    """Simplify geometries to reduce data size for map rendering.

    Uses Douglas-Peucker simplification. A tolerance of 0.001 degrees
    (~100 m) works well for state-level maps without visible loss.
    """
    if gdf is None or gdf.empty:
        return gdf
    result = gdf.copy()
    result["geometry"] = result.geometry.simplify(tolerance, preserve_topology=True)
    return result


def convert_to_centroids(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Convert all geometries in a GeoDataFrame to their centroids.
    Useful for converting polygons to points for marker display.

    Args:
        gdf: GeoDataFrame with any geometry types

    Returns:
        GeoDataFrame with point geometries (centroids)
    """
    if gdf is None or gdf.empty:
        return gdf

    result = gdf.copy()
    result['geometry'] = result.geometry.centroid
    return result
