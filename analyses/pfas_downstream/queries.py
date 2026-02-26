"""
PFAS Downstream Tracing Query Functions
Implements a 3-step pipeline:
    Step 1: Find facilities by NAICS industry type in a region
    Step 2: Find downstream flowlines/streams from facilities
    Step 3: Find samplepoints in downstream S2 cells
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from core.sparql import (
    ENDPOINT_URLS,
    parse_sparql_results,
    post_sparql_with_debug,
    build_county_region_filter,
    build_facility_values,
    concentration_filter_sparql,
)
from core.naics_utils import normalize_naics_codes, build_naics_values_and_hierarchy


def _build_industry_filter(naics_code: Optional[str]) -> tuple[str, str]:
    """
    Build NAICS VALUES clause and hierarchy for downstream queries.

    Supports all NAICS levels:
      - 2 digits (sector): builds full hierarchy chain
      - 3 digits (subsector): builds hierarchy to subsector
      - 4 digits (group): binds ?industryGroup directly
      - 5-6 digits (industry): binds ?industryCode directly

    Returns:
        (industry_values, industry_hierarchy) tuple
    """
    codes = normalize_naics_codes(naics_code)
    if not codes:
        return "", ""
    return build_naics_values_and_hierarchy(codes[0])


def _build_downstream_facility_region_filter(region_code: Optional[str], county_var: str = "?facCounty") -> str:
    """
    Region filter scoped to facility-connected county variable for downstream Step 3.

    Matches the requested pattern:
      ?facCounty rdf:type kwg-ont:AdministrativeRegion_3 ;
                 kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.<code> .
    """
    code = str(region_code or "").strip()
    if not code:
        return ""
    if len(code) > 5:
        # Downstream Step 3 facility filter expects state/county style codes.
        return ""
    return (
        f"{county_var} rdf:type kwg-ont:AdministrativeRegion_3 ;\n"
        f"               kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{code} ."
    )


def execute_downstream_facilities_query(
    naics_code: Optional[str],
    region_code: Optional[str],
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 1: Find facilities by NAICS industry type in a region."""
    industry_values, industry_hierarchy = _build_industry_filter(naics_code)
    region_filter = build_county_region_filter(region_code, county_var="?facCounty")

    if not industry_values:
        return pd.DataFrame(), "Industry type is required for downstream tracing", {"error": "No industry selected"}

    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industryCode ?industryName WHERE {{
    ?facility fio:ofIndustry ?industryGroup;
        fio:ofIndustry ?industryCode ;
        spatial:connectedTo ?facCounty ;
        geo:hasGeometry/geo:asWKT ?facWKT;
        rdfs:label ?facilityName.
    {region_filter}
    ?industryCode a naics:NAICS-IndustryCode;
        fio:subcodeOf ?industryGroup ;
        rdfs:label ?industryName.
    {industry_hierarchy}
    {industry_values}
}}
"""
    results_json, error, debug_info = post_sparql_with_debug("federation", query)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info
    df = parse_sparql_results(results_json)
    return df, None, debug_info


def execute_downstream_streams_query(
    naics_code: Optional[str],
    region_code: Optional[str],
    facility_uris: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 2: Find downstream flowlines/streams from facilities."""
    if facility_uris is not None and not isinstance(facility_uris, list):
        facility_uris = None

    facility_values_clause = build_facility_values(facility_uris)
    industry_values, industry_hierarchy = _build_industry_filter(naics_code)
    region_filter = build_county_region_filter(region_code, county_var="?facCounty")

    if facility_values_clause:
        industry_values = ""
        industry_hierarchy = ""
        region_filter = ""
    elif not industry_values:
        return pd.DataFrame(), "Industry type is required", {"error": "No industry selected"}

    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?downstream_flowline ?dsflWKT ?fl_type ?streamName
WHERE {{
    {{SELECT ?s2 WHERE {{
        ?s2 spatial:connectedTo ?facility.
        {facility_values_clause}
        ?facility fio:ofIndustry ?industryGroup;
            fio:ofIndustry ?industryCode;
            spatial:connectedTo ?facCounty.
        {region_filter}
        ?industryCode a naics:NAICS-IndustryCode;
            fio:subcodeOf ?industryGroup ;
            rdfs:label ?industryName.
        {industry_hierarchy}
        {industry_values}
    }}}}

    ?s2 kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
    ?s2neighbor rdf:type kwg-ont:S2Cell_Level13;
              spatial:connectedTo ?upstream_flowline.

    ?upstream_flowline rdf:type hyf:HY_FlowPath ;
              hyf:downstreamFlowPathTC ?downstream_flowline .
    ?downstream_flowline geo:hasGeometry/geo:asWKT ?dsflWKT;
              nhdplusv2:hasFTYPE ?fl_type.
    OPTIONAL {{?downstream_flowline rdfs:label ?streamName}}
}}
"""
    results_json, error, debug_info = post_sparql_with_debug("federation", query)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info
    df = parse_sparql_results(results_json)
    return df, None, debug_info


def execute_downstream_samples_query(
    naics_code: Optional[str],
    region_code: Optional[str],
    facility_uris: Optional[List[str]] = None,
    min_conc: float = 0.0,
    max_conc: float = 500.0,
    include_nondetects: bool = False,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 3: Find contaminated samples downstream of facilities."""
    if facility_uris is not None and not isinstance(facility_uris, list):
        facility_uris = None

    facility_values_clause = build_facility_values(facility_uris)
    industry_values, industry_hierarchy = _build_industry_filter(naics_code)
    facility_region_filter = _build_downstream_facility_region_filter(region_code, county_var="?facCounty")

    if facility_values_clause:
        industry_values = ""
        industry_hierarchy = ""
        facility_region_filter = ""
    elif not industry_values:
        return pd.DataFrame(), "Industry type is required", {"error": "No industry selected"}

    conc_filter = concentration_filter_sparql(min_conc, max_conc, include_nondetects)

    query = f"""
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?samplePoint ?spWKT ?sample
    (GROUP_CONCAT(DISTINCT ?sampleId; separator="; ") as ?samples)
    (COUNT(DISTINCT ?subVal) as ?resultCount)
    (MAX(?numericValue) as ?Max)
    ?unit
    (GROUP_CONCAT(DISTINCT ?subVal; separator=" <br> ") as ?results)
WHERE {{
    {{ SELECT DISTINCT ?s2cell WHERE {{
        ?s2origin spatial:connectedTo ?facility.
        {facility_values_clause}
        ?facility fio:ofIndustry ?industryGroup;
            fio:ofIndustry ?industryCode;
            spatial:connectedTo ?facCounty.
        {facility_region_filter}
        ?industryCode a naics:NAICS-IndustryCode;
            fio:subcodeOf ?industryGroup ;
            rdfs:label ?industryName.
        {industry_hierarchy}
        {industry_values}

        ?s2origin kwg-ont:sfTouches|owl:sameAs ?s2neighbor.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13;
              spatial:connectedTo ?upstream_flowline.

        ?upstream_flowline rdf:type hyf:HY_FlowPath ;
              hyf:downstreamFlowPathTC ?downstream_flowline .
        ?s2cell spatial:connectedTo ?downstream_flowline ;
              rdf:type kwg-ont:S2Cell_Level13 .
    }}}}

    ?samplePoint spatial:connectedTo ?s2cell ;
        rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT .
    ?s2cell rdf:type kwg-ont:S2Cell_Level13.
    ?sample coso:fromSamplePoint ?samplePoint;
        dcterms:identifier ?sampleId;
        coso:sampleOfMaterialType/rdfs:label ?type.
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?samplePoint;
        coso:ofDSSToxSubstance/skos:altLabel ?substance;
        coso:hasResult ?res .
    ?res coso:measurementValue ?result_value;
        coso:measurementUnit/qudt:symbol ?unit.
    OPTIONAL {{ ?res qudt:quantityValue/qudt:numericValue ?numericResult }}
    OPTIONAL {{ ?res qudt:enumeratedValue ?enumDetected }}
    BIND(
      (BOUND(?enumDetected) || LCASE(STR(?result_value)) = "non-detect" || STR(?result_value) = STR(coso:non-detect))
      as ?isNonDetect
    )
    BIND(
      IF(
        ?isNonDetect,
        0,
        COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value))
      ) as ?numericValue
    )
    {conc_filter}
    BIND((CONCAT(?substance, ": ", str(?result_value) , " ", ?unit) ) as ?subVal)

}} GROUP BY ?samplePoint ?spWKT ?sample ?unit
"""
    results_json, error, debug_info = post_sparql_with_debug("federation", query)
    if error or not results_json:
        return pd.DataFrame(), error, debug_info
    df = parse_sparql_results(results_json)
    return df, None, debug_info
