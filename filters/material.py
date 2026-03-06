"""
Material Type Filtering Utilities
SPARQL queries to filter material types based on available observations in a region
"""
from __future__ import annotations

from typing import List
import pandas as pd
import streamlit as st

from core.sparql import ENDPOINT_URLS, parse_sparql_results, execute_sparql_query


def _fallback_material_name(material_uri: str) -> str:
    return material_uri.rstrip("/").rsplit("/", 1)[-1]


def get_available_material_types_with_labels(
    region_code: str,
    is_subdivision: bool = False,
) -> pd.DataFrame:
    """
    Get all material types that have observations in the given region.
    Only includes material types with URIs starting with http://w3id.org/.
    Returns a DataFrame with material URI and display name.

    Args:
        region_code: FIPS code for the region (county or subdivision)
        is_subdivision: True if region_code is a subdivision (uses DataCommons URI format)

    Returns:
        DataFrame with columns: matType, display_name
    """
    if is_subdivision:
        query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?matType ?matTypeLabel WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches <https://datacommons.org/browser/geoId/{region_code}> .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:analyzedSample ?sample .
    ?sample coso:sampleOfMaterialType ?matType .
    OPTIONAL {{ ?matType rdfs:label ?matTypeLabel . }}
    FILTER(STRSTARTS(STR(?matType), "http://w3id.org/")).
}}
"""
    else:
        query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?matType ?matTypeLabel WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf+ <http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{region_code}> .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:analyzedSample ?sample .
    ?sample coso:sampleOfMaterialType ?matType .
    OPTIONAL {{ ?matType rdfs:label ?matTypeLabel . }}
    FILTER(STRSTARTS(STR(?matType), "http://w3id.org/")).
}}
"""

    results = execute_sparql_query(ENDPOINT_URLS["federation"], query)
    if not results:
        return pd.DataFrame(columns=["matType", "display_name"])

    df = parse_sparql_results(results)
    if df.empty:
        return pd.DataFrame(columns=["matType", "display_name"])

    df = df.dropna(subset=["matType"]).copy()
    df["has_label"] = df["matTypeLabel"].notna()
    df = df.sort_values("has_label", ascending=False)
    df = df.drop_duplicates(subset=["matType"], keep="first")
    df["display_name"] = df["matTypeLabel"]
    df["display_name"] = df["display_name"].where(
        df["display_name"].notna(),
        df["matType"].apply(_fallback_material_name),
    )
    return df[["matType", "display_name"]].reset_index(drop=True)


@st.cache_data(ttl=3600)
def get_cached_material_types_with_labels(
    region_code: str,
    is_subdivision: bool = False,
) -> pd.DataFrame:
    """Cached wrapper for region-scoped sample material availability."""
    return get_available_material_types_with_labels(region_code, is_subdivision)


def get_available_material_types(region_code: str, is_subdivision: bool = False) -> List[str]:
    """
    Get all material types that have observations in the given region.
    Only includes material types with URIs starting with http://w3id.org/

    Args:
        region_code: FIPS code for the region (county or subdivision)
        is_subdivision: True if region_code is a subdivision (uses DataCommons URI format)

    Returns:
        List of material type URIs
    """
    df = get_available_material_types_with_labels(region_code, is_subdivision)
    if df.empty:
        return []
    return df["matType"].tolist()
