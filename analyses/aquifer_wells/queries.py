"""
Aquifer-Connected Wells Query Functions

Single-query pipeline matching the notebook approach:
    Find PFAS-contaminated sample points, aquifers spatially connected to those
    sample points via S2 cells, and Maine MGS water wells connected to those
    aquifers via S2 cells.  All in one federated SPARQL query.

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


def execute_aquifer_query(
    region_code: Optional[str],
    substance_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    include_nondetects: bool = False,
    timeout: Optional[int] = None,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Find contaminated sample points, connected aquifers, and water wells.

    Returns a flat DataFrame with one row per (sample, aquifer, well, observation)
    combination.  The caller splits this into thematic DataFrames for display.
    """
    region_clause = _build_region_clause(region_code)
    substance_filter = sparql_values_uri("substance", substance_uri)
    conc_filter = concentration_filter_sparql(min_conc, max_conc, include_nondetects)

    query = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX gwml2: <http://gwml2.org/def/gwml2#>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX me_mgs: <http://sawgraph.spatialai.org/v1/me-mgs#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?sp ?spwkt ?aquifer ?aquiferwkt
       ?well ?wellwkt ?welllabel ?welluse ?welltype ?welldepthft ?welloverburdenft
       ?result_value ?numericValue
WHERE {{
    {region_clause}
    ?samples2 rdf:type kwg-ont:S2Cell_Level13 ;
              spatial:connectedTo ?aquifer .
    ?aquifer rdf:type gwml2:GW_Aquifer ;
             geo:hasGeometry/geo:asWKT ?aquiferwkt .

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
    BIND(REPLACE(str(?welluseiri), "(.*)\\\\.(.*)", "$2") AS ?welluse)
    BIND(REPLACE(str(?welltypeiri), "(.*)\\\\.(.*)", "$2") AS ?welltype)
    BIND(CONCAT(str(?welldepth), " ft") AS ?welldepthft)
    BIND(CONCAT(str(?welloverburden), " ft") AS ?welloverburdenft)

    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spwkt ;
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
}}
"""
    js, error, debug_info = post_sparql_with_debug("federation", query, timeout=timeout)
    df = parse_sparql_results(js) if js else pd.DataFrame()
    if not df.empty and "numericValue" in df.columns:
        df["numericValue"] = pd.to_numeric(df["numericValue"], errors="coerce")
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
