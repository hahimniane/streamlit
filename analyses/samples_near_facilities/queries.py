"""
Nearby Samples Queries
Find PFAS samples near specific facility types (by NAICS code).
"""
from __future__ import annotations

import pandas as pd
from typing import Any, Dict, Optional, Tuple

from core.sparql import (
    concentration_filter_sparql,
    parse_sparql_results,
    post_sparql_with_debug,
)
from core.naics_utils import build_naics_values_and_hierarchy, normalize_naics_codes



def _build_industry_filter(naics_code: str | list[str]) -> tuple[str, str]:
    naics_codes = normalize_naics_codes(naics_code)
    if not naics_codes:
        return "", ""
    return build_naics_values_and_hierarchy(naics_codes[0])


def _build_region_filter(region_code: Optional[str]) -> str:
    """
    Build facility county region filter.
    - state (2 digits): keep counties in selected state
    - county (5 digits): restrict to that county
    """
    sanitized_region = str(region_code).strip() if region_code else ""
    if not sanitized_region:
        return ""

    if len(sanitized_region) == 2:
        return f"""
    ?county rdf:type kwg-ont:AdministrativeRegion_2 ;
            kwg-ont:administrativePartOf kwgr:administrativeRegion.USA.{sanitized_region} .
"""
    if len(sanitized_region) == 5:
        return f"""
    VALUES ?county {{ kwgr:administrativeRegion.USA.{sanitized_region} }} .
"""
    return ""


def execute_nearby_facilities_query(
    naics_code: str | list[str],
    region_code: Optional[str],
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 1: Find facilities in selected industry/region."""
    industry_values, industry_hierarchy = _build_industry_filter(naics_code)
    region_filter = _build_region_filter(region_code)

    query = f"""
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industryCode ?industryName WHERE {{
    ?facility fio:ofIndustry ?industryGroup;
              fio:ofIndustry ?industryCode;
              spatial:connectedTo ?county;
              geo:hasGeometry/geo:asWKT ?facWKT;
              rdfs:label ?facilityName.
    {region_filter}
    ?industryCode a naics:NAICS-IndustryCode;
                  fio:subcodeOf ?industryGroup;
                  rdfs:label ?industryName.
    {industry_hierarchy}
    {industry_values}
}}
"""
    results_json, error, debug_info = post_sparql_with_debug("federation", query)
    facilities_df = parse_sparql_results(results_json) if results_json else pd.DataFrame()
    debug_info.update(
        {
            "label": "Step 1: Facilities",
            "error": error,
            "row_count": len(facilities_df),
        }
    )
    return facilities_df, error, debug_info


def execute_nearby_samples_query(
    naics_code: str | list[str],
    region_code: Optional[str],
    min_concentration: float = 0.0,
    max_concentration: float = 500.0,
    include_nondetects: bool = False,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 2: Find raw per-observation PFAS sample rows near industry facilities.

    Returns one row per observation with columns: samplePoint, samplePointName,
    spWKT, sample, sampleIdentifier, date, substance, result, unit, sampleType.
    """
    industry_values, industry_hierarchy = _build_industry_filter(naics_code)
    region_filter = _build_region_filter(region_code)
    conc_filter = concentration_filter_sparql(min_concentration, max_concentration, include_nondetects)

    query = f"""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX qudt: <http://qudt.org/schema/qudt/>

SELECT DISTINCT ?samplePoint ?samplePointName ?spWKT
    ?sample ?sampleIdentifier ?date ?substance ?result ?unit ?sampleType
WHERE {{

    {{SELECT DISTINCT ?s2neighbor WHERE {{
        ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
                kwg-ont:sfContains ?facility.
        ?facility fio:ofIndustry ?industryGroup;
                  fio:ofIndustry ?industryCode;
                  spatial:connectedTo ?county .
        {region_filter}
        ?industryCode a naics:NAICS-IndustryCode;
                      fio:subcodeOf ?industryGroup;
                      rdfs:label ?industryName.
        {industry_values}
        {industry_hierarchy}
        ?s2neighbor kwg-ont:sfTouches|owl:sameAs ?s2cell.
        ?s2neighbor rdf:type kwg-ont:S2Cell_Level13 .
    }} }}

    ?samplePoint rdf:type coso:SamplePoint;
        spatial:connectedTo ?s2neighbor;
        geo:hasGeometry/geo:asWKT ?spWKT.
    OPTIONAL {{ ?samplePoint rdfs:label ?samplePointName }}
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?samplePoint;
        coso:ofSubstance ?substance1;
        coso:analyzedSample ?sample;
        coso:hasResult ?resultNode.
    OPTIONAL {{ ?observation coso:observedTime ?date }}
    OPTIONAL {{ ?sample dcterms:identifier ?sampleIdentifier }}
    OPTIONAL {{ ?sample coso:sampleOfMaterialType/rdfs:label ?sampleType }}
    ?resultNode coso:measurementValue ?result;
               coso:measurementUnit ?unitURI.
    OPTIONAL {{ ?resultNode qudt:quantityValue/qudt:numericValue ?numericResult }}
    OPTIONAL {{ ?resultNode qudt:enumeratedValue ?enumDetected }}
    BIND(
      (BOUND(?enumDetected) || LCASE(STR(?result)) = "non-detect" || STR(?result) = STR(coso:non-detect))
      as ?isNonDetect
    )
    BIND(
      IF(
        ?isNonDetect,
        0,
        COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result))
      ) as ?numericValue
    )
    ?substance1 rdfs:label ?substance.
    ?unitURI qudt:symbol ?unit.
    {conc_filter}
}}
"""

    results_json, error, debug_info = post_sparql_with_debug("federation", query)
    samples_df = parse_sparql_results(results_json) if results_json else pd.DataFrame()
    debug_info.update(
        {
            "label": "Step 2: Nearby Samples",
            "error": error,
            "row_count": len(samples_df),
        }
    )
    return samples_df, error, debug_info


