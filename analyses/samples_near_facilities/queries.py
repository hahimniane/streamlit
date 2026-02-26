"""
Nearby Samples Queries
Find PFAS samples near specific facility types (by NAICS code).
"""
from __future__ import annotations

import pandas as pd
from typing import Any, Dict, Optional, Tuple

from core.sparql import (
    ENDPOINT_URLS,
    concentration_filter_sparql,
    parse_sparql_results,
    post_sparql_with_debug,
)
from core.naics_utils import build_naics_values_and_hierarchy, normalize_naics_codes


# Alias for backward compatibility
ENDPOINTS = ENDPOINT_URLS


def _normalize_samples_df(samples_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize sample columns to a common shape for UI display."""
    if samples_df.empty:
        return samples_df

    if "max" not in samples_df.columns and "maxConcentration" in samples_df.columns:
        samples_df = samples_df.rename(columns={"maxConcentration": "max"})
    if "Materials" not in samples_df.columns and "materials" in samples_df.columns:
        samples_df = samples_df.rename(columns={"materials": "Materials"})
    if "results" not in samples_df.columns and "substances" in samples_df.columns:
        samples_df["results"] = samples_df["substances"]
    if "datedresults" not in samples_df.columns:
        samples_df["datedresults"] = ""
    if "dates" not in samples_df.columns:
        samples_df["dates"] = ""
    if "Type" not in samples_df.columns:
        samples_df["Type"] = ""

    for col in ("max", "resultCount"):
        if col in samples_df.columns:
            samples_df[col] = pd.to_numeric(samples_df[col], errors="coerce")

    return samples_df


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
    """Step 2: Find PFAS samples near industry facilities."""
    industry_values, industry_hierarchy = _build_industry_filter(naics_code)
    region_filter = _build_region_filter(region_code)

    if include_nondetects:
        concentration_filter = concentration_filter_sparql(min_concentration, max_concentration, True)
        nondetect_fragment = """
    OPTIONAL { ?result qudt:enumeratedValue ?enumDetected }
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
"""
    else:
        concentration_filter = "\n".join(
            [
                "FILTER(BOUND(?numericValue))",
                "FILTER(?numericValue > 0)",
                f"FILTER (?numericValue >= {min_concentration} && ?numericValue <= {max_concentration})",
            ]
        )
        nondetect_fragment = """
    BIND(COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value)) as ?numericValue)
"""

    query = f"""
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
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

SELECT DISTINCT (COUNT(DISTINCT ?observation) as ?resultCount) (MAX(?numericValue) as ?max) (GROUP_CONCAT(DISTINCT ?subVal; separator="</br>") as ?results) (GROUP_CONCAT(DISTINCT ?datedSubVal; separator="</br>") as ?datedresults) (GROUP_CONCAT(?year; separator=" </br> ") as ?dates) (GROUP_CONCAT(DISTINCT ?Typelabels; separator=";") as ?Type) (GROUP_CONCAT(DISTINCT ?material) as ?Materials) ?sp ?spName ?spWKT
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

    ?sp rdf:type coso:SamplePoint;
        spatial:connectedTo ?s2neighbor;
        rdfs:label ?spName;
        geo:hasGeometry/geo:asWKT ?spWKT.
    ?observation rdf:type coso:ContaminantObservation;
        coso:observedAtSamplePoint ?sp;
        coso:ofSubstance ?substance1;
        coso:observedTime ?time;
        coso:analyzedSample ?sample;
        coso:hasResult ?result.
    ?sample rdfs:label ?sampleLabel;
            coso:sampleOfMaterialType/rdfs:label ?material.
    {{SELECT ?sample (GROUP_CONCAT(DISTINCT ?sampleClassLabel; separator=";") as ?Typelabels) WHERE {{
        ?sample a ?sampleClass.
        ?sampleClass rdfs:label ?sampleClassLabel.
        VALUES ?sampleClass {{coso:WaterSample coso:AnimalMaterialSample coso:PlantMaterialSample coso:SolidMaterialSample}}
    }} GROUP BY ?sample }}
    ?result coso:measurementValue ?result_value;
            coso:measurementUnit ?unit.
    OPTIONAL {{ ?result qudt:quantityValue/qudt:numericValue ?numericResult }}
    {nondetect_fragment}
    ?substance1 rdfs:label ?substance.
    ?unit qudt:symbol ?unit_sym.
    {concentration_filter}
    BIND(SUBSTR(?time, 1, 7) as ?year)
    BIND(CONCAT('<b>',str(?result_value), '</b>', " ", ?unit_sym, " ", ?substance) as ?subVal)
    BIND(CONCAT(?year, ' <b> ',str(?result_value), '</b>', " ", ?unit_sym, " ", ?substance) as ?datedSubVal)
}} GROUP BY ?sp ?spName ?spWKT
ORDER BY DESC(?max)
"""

    results_json, error, debug_info = post_sparql_with_debug("federation", query)
    samples_df = parse_sparql_results(results_json) if results_json else pd.DataFrame()
    if not samples_df.empty:
        samples_df = _normalize_samples_df(samples_df)
    debug_info.update(
        {
            "label": "Step 2: Nearby Samples",
            "error": error,
            "row_count": len(samples_df),
        }
    )
    return samples_df, error, debug_info


def execute_nearby_analysis(
    naics_code: str | list[str],
    region_code: Optional[str],
    min_concentration: float = 0.0,
    max_concentration: float = 500.0,
    include_nondetects: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Compatibility wrapper that runs both nearby queries and returns old shape.
    """
    facilities_df, facilities_error, facilities_debug = execute_nearby_facilities_query(
        naics_code=naics_code,
        region_code=region_code,
    )
    samples_df, samples_error, samples_debug = execute_nearby_samples_query(
        naics_code=naics_code,
        region_code=region_code,
        min_concentration=min_concentration,
        max_concentration=max_concentration,
        include_nondetects=include_nondetects,
    )

    debug_info: Dict[str, Any] = {
        "queries": [facilities_debug, samples_debug],
        "errors": [e for e in [facilities_error, samples_error] if e],
    }
    return facilities_df, samples_df, debug_info
