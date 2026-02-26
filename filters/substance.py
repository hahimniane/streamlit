"""
Substance Filtering Utilities
SPARQL queries to filter substances based on available observations in a region
"""
from __future__ import annotations

from functools import lru_cache
from typing import List, Optional
import pandas as pd
import requests
import streamlit as st

from core.sparql import ENDPOINT_URLS, parse_sparql_results, execute_sparql_query


# External API endpoints
COMPTox_DSS_TOX_ENDPOINT = (
    "https://comptox.epa.gov/dashboard-api/ccdapp2/chemical-detail/search/by-dsstoxsid"
)
WQP_CHARACTERISTIC_ENDPOINT = "https://www.waterqualitydata.us/Codes/characteristicname"
DSSTOX_URI_PREFIX = "http://w3id.org/DSSTox/v1/DTXSID"
WQP_URI_FRAGMENT = "#characteristic."
WQP_LABEL_LOOKUP_ENABLED = True


def _fallback_substance_name(substance_uri: str) -> str:
    cleaned = substance_uri.rstrip("/")
    if "#" in cleaned:
        return cleaned.rsplit("#", 1)[-1]
    return cleaned.rsplit("/", 1)[-1]


def _extract_dsstox_id(substance_uri: str) -> Optional[str]:
    if substance_uri.startswith(DSSTOX_URI_PREFIX):
        return _fallback_substance_name(substance_uri)
    return None


def _extract_wqp_id(substance_uri: str) -> Optional[str]:
    if WQP_URI_FRAGMENT in substance_uri:
        return substance_uri.split(WQP_URI_FRAGMENT, 1)[-1]
    return None


@lru_cache(maxsize=2048)
def _fetch_comptox_label(dsstox_id: str) -> Optional[str]:
    try:
        response = requests.get(
            COMPTox_DSS_TOX_ENDPOINT,
            params={"id": dsstox_id},
            timeout=30
        )
        if response.status_code != 200:
            return None
        data = response.json()
    except Exception:
        return None

    label = data.get("label") or data.get("preferredName")
    if not isinstance(label, str) or not label.strip():
        return None
    return label.strip()


@lru_cache(maxsize=2048)
def _fetch_wqp_label(characteristic_id: str) -> Optional[str]:
    global WQP_LABEL_LOOKUP_ENABLED
    if not WQP_LABEL_LOOKUP_ENABLED:
        return None
    try:
        response = requests.get(
            WQP_CHARACTERISTIC_ENDPOINT,
            params={"characteristicID": characteristic_id},
            headers={"Accept": "application/json"},
            timeout=30
        )
        if response.status_code != 200:
            return None
        data = response.json()
    except Exception:
        return None

    codes = data.get("codes")
    if not isinstance(codes, list) or not codes:
        return None

    id_keys = ("characteristicID", "characteristicId", "id", "code")
    has_id_field = any(
        isinstance(item, dict) and any(k in item for k in id_keys)
        for item in codes[:50]
    )
    if not has_id_field:
        WQP_LABEL_LOOKUP_ENABLED = False
        return None

    for item in codes:
        if not isinstance(item, dict):
            continue
        for key in id_keys:
            if str(item.get(key)) == characteristic_id:
                value = item.get("value") or item.get("name")
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return None


def get_available_substances_with_labels(
    region_code: str,
    is_subdivision: bool = False,
) -> pd.DataFrame:
    """
    Get all substances that have observations in the given region.
    Only includes substances with URIs starting with http://w3id.org/.
    Returns a DataFrame with substance URI and display name.

    Args:
        region_code: FIPS code for the region (county or subdivision)
        is_subdivision: True if region_code is a subdivision (uses DataCommons URI format)

    Returns:
        DataFrame with columns: substance, display_name
    """
    if is_subdivision:
        query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?substance ?label WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches <https://datacommons.org/browser/geoId/{region_code}> .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance .
    OPTIONAL {{ ?substance rdfs:label ?label . }}
    FILTER(STRSTARTS(STR(?substance), "http://w3id.org/")).
}}
"""
    else:
        query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?substance ?label WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        kwg-ont:sfWithin|kwg-ont:sfTouches ?ar3 .
    ?ar3 rdf:type kwg-ont:AdministrativeRegion_3 ;
         kwg-ont:administrativePartOf <http://stko-kwg.geog.ucsb.edu/lod/resource/administrativeRegion.USA.{region_code}> .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance .
    OPTIONAL {{ ?substance rdfs:label ?label . }}
    FILTER(STRSTARTS(STR(?substance), "http://w3id.org/")).
}}
"""

    results = execute_sparql_query(ENDPOINT_URLS["federation"], query)
    if not results:
        return pd.DataFrame(columns=["substance", "display_name"])

    df = parse_sparql_results(results)
    if df.empty:
        return pd.DataFrame(columns=["substance", "display_name"])

    df = df.dropna(subset=["substance"]).copy()
    df["has_label"] = df["label"].notna()
    df = df.sort_values("has_label", ascending=False)
    df = df.drop_duplicates(subset=["substance"], keep="first")
    df["display_name"] = df["label"]
    df["dsstox_id"] = df["substance"].apply(_extract_dsstox_id)
    df["wqp_id"] = df["substance"].apply(_extract_wqp_id)

    def _resolve_display_name(row: pd.Series) -> str:
        if pd.notna(row["display_name"]):
            return row["display_name"]
        dsstox_id = row.get("dsstox_id")
        if isinstance(dsstox_id, str) and dsstox_id:
            comptox_label = _fetch_comptox_label(dsstox_id)
            if comptox_label:
                return comptox_label
        wqp_id = row.get("wqp_id")
        if isinstance(wqp_id, str) and wqp_id:
            wqp_label = _fetch_wqp_label(wqp_id)
            if wqp_label:
                return wqp_label
        return _fallback_substance_name(row["substance"])

    df["display_name"] = df.apply(_resolve_display_name, axis=1)
    return df[["substance", "display_name"]].reset_index(drop=True)


@st.cache_data(ttl=3600)
def get_cached_substances_with_labels(
    region_code: str,
    is_subdivision: bool = False,
) -> pd.DataFrame:
    """Cached wrapper for region-scoped substance availability."""
    return get_available_substances_with_labels(region_code, is_subdivision)


def get_available_substances(region_code: str, is_subdivision: bool = False) -> List[str]:
    """
    Get all substances that have observations in the given region.
    Only includes substances with URIs starting with http://w3id.org/

    Args:
        region_code: FIPS code for the region (county or subdivision)
        is_subdivision: True if region_code is a subdivision (uses DataCommons URI format)

    Returns:
        List of substance URIs
    """
    df = get_available_substances_with_labels(region_code, is_subdivision)
    if df.empty:
        return []
    return df["substance"].tolist()
