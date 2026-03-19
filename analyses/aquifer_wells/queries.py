"""
Aquifer-Connected Wells Query Functions

Split into 3 separate queries:
    1. Sample point observations (raw per-observation rows)
    2. Aquifers connected to tested sample points
    3. Water wells connected to those aquifers

Also provides a drill-down query for the full PFAS measurement history of a
single sample point.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from core.sparql import (
    build_query_debug_entry,
    concentration_filter_sparql,
    parse_sparql_results,
    post_sparql_with_debug,
    region_pattern_sparql,
    sparql_values_uri,
)


def _build_region_clause(region_code: Optional[str]) -> str:
    """Return SPARQL lines that constrain ?samples2 to a region.

    When a region code is provided the clause defines ?regionURI (via
    region_pattern_sparql) and ties the shared S2 cell to it.
    Returns empty string when no region code is given.
    """
    if not region_code or not region_code.strip():
        return ""
    return f"""{region_pattern_sparql(region_code)}
    ?samples2 spatial:connectedTo ?regionURI ."""


def _build_contamination_subquery(
    region_clause: str,
    substance_filter: str,
    conc_filter: str,
) -> str:
    """Build a reusable subquery that finds S2 cells with contaminated EGAD PWSW sample points."""
    return f"""SELECT DISTINCT ?samples2 WHERE {{
        {region_clause}
        ?samples2 rdf:type kwg-ont:S2Cell_Level13 .
        ?sp rdf:type me_egad:EGAD-SamplePoint ;
            me_egad:samplePointType me_egad:featureType.PWSW ;
            kwg-ont:sfWithin ?samples2 .
        ?obs rdf:type coso:ContaminantObservation ;
             coso:observedAtSamplePoint ?sp ;
             coso:ofSubstance ?substance ;
             coso:hasResult ?result .
        ?result coso:measurementValue ?result_value ;
                coso:measurementUnit ?unit .
        OPTIONAL {{ ?result qudt:quantityValue/qudt:numericValue ?numericResult }}
        OPTIONAL {{ ?result qudt:enumeratedValue ?enumDetected }}
        BIND( (BOUND(?enumDetected) || LCASE(STR(?result_value)) = "non-detect" || STR(?result_value) = STR(coso:non-detect)) AS ?isNonDetect )
        BIND( IF(?isNonDetect, 0, COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value))) AS ?numericValue )
        VALUES ?unit {{ <http://qudt.org/vocab/unit/NanoGM-PER-L> }}
        {substance_filter}
        {conc_filter}
    }}"""


_PREFIXES = """PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX gwml2: <http://gwml2.org/def/gwml2#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX me_egad: <http://w3id.org/sawgraph/v1/me-egad#>
PREFIX me_mgs: <http://sawgraph.spatialai.org/v1/me-mgs#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"""


def execute_aquifer_samples_query(
    region_code: Optional[str],
    substance_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    include_nondetects: bool = False,
    timeout: Optional[int] = None,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 1: Find raw per-observation rows for contaminated EGAD PWSW sample points.

    Returns one row per observation with columns: samplePoint, samplePointName,
    spWKT, sample, sampleIdentifier, date, substance, result, unit, sampleType.
    """
    region_clause = _build_region_clause(region_code)
    substance_filter = sparql_values_uri("substance", substance_uri)
    conc_filter = concentration_filter_sparql(min_conc, max_conc, include_nondetects)

    query = f"""{_PREFIXES}

SELECT DISTINCT ?samplePoint ?samplePointName ?spWKT
       ?sample ?sampleIdentifier ?date ?substance ?result ?unit ?sampleType
WHERE {{
    {region_clause}
    ?samples2 rdf:type kwg-ont:S2Cell_Level13 ;
              spatial:connectedTo ?aquifer .
    ?aquifer rdf:type gwml2:GW_Aquifer .

    ?samplePoint rdf:type me_egad:EGAD-SamplePoint ;
        me_egad:samplePointType me_egad:featureType.PWSW ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        kwg-ont:sfWithin ?samples2 .
    OPTIONAL {{ ?samplePoint rdfs:label ?samplePointName }}

    ?obs rdf:type coso:ContaminantObservation ;
         coso:observedAtSamplePoint ?samplePoint ;
         coso:ofSubstance ?substanceURI ;
         coso:hasResult ?resultNode .
    ?substanceURI rdfs:label|skos:altLabel ?substance .
    ?resultNode coso:measurementValue ?result ;
               coso:measurementUnit ?unitURI .
    ?unitURI qudt:symbol ?unit .
    OPTIONAL {{ ?resultNode qudt:quantityValue/qudt:numericValue ?numericResult }}
    OPTIONAL {{ ?resultNode qudt:enumeratedValue ?enumDetected }}
    BIND( (BOUND(?enumDetected) || LCASE(STR(?result)) = "non-detect" || STR(?result) = STR(coso:non-detect)) AS ?isNonDetect )
    BIND( IF(?isNonDetect, 0, COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result))) AS ?numericValue )
    VALUES ?unitURI {{ <http://qudt.org/vocab/unit/NanoGM-PER-L> }}
    {substance_filter}
    {conc_filter}

    OPTIONAL {{ ?obs coso:analyzedSample ?sample }}
    OPTIONAL {{ ?sample dcterms:identifier ?sampleIdentifier }}
    OPTIONAL {{ ?obs coso:observedTime ?date }}
    OPTIONAL {{ ?sample coso:sampleOfMaterialType/rdfs:label ?sampleType }}
}}
"""
    js, error, debug_info = post_sparql_with_debug("federation", query, timeout=timeout)
    df = parse_sparql_results(js) if js else pd.DataFrame()
    debug_info.update({"label": "Step 1: Sample Observations", "error": error, "row_count": len(df)})
    return df, error, debug_info


def execute_aquifer_aquifers_query(
    region_code: Optional[str],
    substance_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    include_nondetects: bool = False,
    timeout: Optional[int] = None,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 2: Find aquifers connected to tested sample points.

    Returns one row per aquifer with columns: aquifer, aquiferwkt.
    """
    region_clause = _build_region_clause(region_code)
    substance_filter = sparql_values_uri("substance", substance_uri)
    conc_filter = concentration_filter_sparql(min_conc, max_conc, include_nondetects)
    contamination_subquery = _build_contamination_subquery(region_clause, substance_filter, conc_filter)

    query = f"""{_PREFIXES}

SELECT DISTINCT ?aquifer ?aquiferwkt
WHERE {{
    {{ {contamination_subquery} }}
    ?samples2 rdf:type kwg-ont:S2Cell_Level13 ;
              spatial:connectedTo ?aquifer .
    ?aquifer rdf:type gwml2:GW_Aquifer ;
             geo:hasGeometry/geo:asWKT ?aquiferwkt .
}}
"""
    js, error, debug_info = post_sparql_with_debug("federation", query, timeout=timeout)
    df = parse_sparql_results(js) if js else pd.DataFrame()
    debug_info.update({"label": "Step 2: Aquifers", "error": error, "row_count": len(df)})
    return df, error, debug_info


def execute_aquifer_wells_query(
    region_code: Optional[str],
    substance_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    include_nondetects: bool = False,
    timeout: Optional[int] = None,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step 3: Find wells connected to aquifers that are connected to tested sample points.

    Returns one row per well with columns: well, wellwkt, welllabel,
    Well Use, Well Type, Well Depth (ft), Overburden (ft).
    """
    region_clause = _build_region_clause(region_code)
    substance_filter = sparql_values_uri("substance", substance_uri)
    conc_filter = concentration_filter_sparql(min_conc, max_conc, include_nondetects)
    contamination_subquery = _build_contamination_subquery(region_clause, substance_filter, conc_filter)

    query = f"""{_PREFIXES}

SELECT DISTINCT ?well ?wellwkt ?welllabel ?welluseiri ?welltypeiri ?welldepth ?welloverburden
WHERE {{
    {{ {contamination_subquery} }}
    ?samples2 rdf:type kwg-ont:S2Cell_Level13 ;
              spatial:connectedTo ?aquifer .
    ?aquifer rdf:type gwml2:GW_Aquifer .

    ?aqs2 rdf:type kwg-ont:S2Cell_Level13 ;
          spatial:connectedTo ?aquifer .
    ?well rdf:type me_mgs:MGS-Well ;
          kwg-ont:sfWithin ?aqs2 ;
          rdfs:label ?welllabel ;
          me_mgs:hasUse ?welluseiri ;
          me_mgs:ofWellType ?welltypeiri ;
          me_mgs:wellDepth/qudt:numericValue ?welldepth ;
          me_mgs:wellOverburden/qudt:numericValue ?welloverburden ;
          geo:hasGeometry/geo:asWKT ?wellwkt .
}}
"""
    js, error, debug_info = post_sparql_with_debug("federation", query, timeout=timeout)
    df = parse_sparql_results(js) if js else pd.DataFrame()

    if not df.empty:
        # Extract local name from IRI (portion after last ".")
        for iri_col, name_col in [("welluseiri", "Well Use"), ("welltypeiri", "Well Type")]:
            if iri_col in df.columns:
                df[name_col] = df[iri_col].str.extract(r'\.([^.]+)$', expand=False)
                df.drop(columns=[iri_col], inplace=True)
        # Convert depth columns to numeric and rename with units
        for raw_col, display_col in [("welldepth", "Well Depth (ft)"), ("welloverburden", "Overburden (ft)")]:
            if raw_col in df.columns:
                df[display_col] = pd.to_numeric(df[raw_col], errors="coerce")
                df.drop(columns=[raw_col], inplace=True)

    debug_info.update({"label": "Step 3: Connected Wells", "error": error, "row_count": len(df)})
    return df, error, debug_info


def execute_sample_history_query(
    sample_point_uri: str,
    timeout: Optional[int] = None,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Drill-down: fetch the full PFAS measurement history for a single sample point."""
    uri_safe = sample_point_uri.strip()
    if not uri_safe.startswith("<"):
        uri_safe = f"<{uri_safe}>"

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?obs_date ?substance_label ?result_value
WHERE {{
    VALUES ?sp {{ {uri_safe} }}
    ?obs rdf:type coso:ContaminantObservation ;
         coso:observedAtSamplePoint ?sp ;
         coso:ofSubstance ?substance ;
         coso:observedTime ?obs_date ;
         coso:hasResult ?result .
    ?result coso:measurementValue ?result_value .
    OPTIONAL {{ ?substance rdfs:label ?substance_label }}
}}
ORDER BY ?obs_date ?substance_label
"""
    js, error, debug_info = post_sparql_with_debug("federation", query, timeout=timeout)
    df = parse_sparql_results(js) if js else pd.DataFrame()
    return df, error, debug_info


def get_aquifer_state_code_set() -> set:
    """Return the set of state FIPS codes that have aquifer-connected well data.
    Currently only Maine (23) has MGS well data in the knowledge graph."""
    return {"23"}
