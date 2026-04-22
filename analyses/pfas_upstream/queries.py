"""
PFAS Upstream Tracing Query Functions

All queries run against the federation endpoint with self-contained SPARQL
(subqueries). No passing of S2 cell lists between steps, so no S2 list limit.

Returns: (samples_df, upstream_s2_df, upstream_flowlines_df, facilities_df, executed_queries, error).
executed_queries entries include "query" (exact SPARQL sent) for debug display.
"""
from __future__ import annotations

from typing import Optional, Tuple
import pandas as pd

from core.sparql import (
    build_query_debug_entry,
    concentration_filter_sparql,
    parse_sparql_results,
    post_sparql_with_debug,
    region_pattern_sparql,
    sparql_values_uri,
)
from core.naics_utils import build_naics_values_and_hierarchy, normalize_naics_codes


def _build_upstream_industry_filter(naics_code: Optional[str]) -> tuple[str, str]:
    """
    Build NAICS VALUES/hierarchy fragments for upstream Step 3 facilities filter.

    The upstream selector can emit virtual values like "31-33"; those are treated
    as invalid here and no industry filter is applied.
    """
    codes = normalize_naics_codes(naics_code)
    if not codes:
        return "", ""

    code = str(codes[0]).strip()
    if not code.isdigit() or len(code) < 2 or len(code) > 6:
        return "", ""

    return build_naics_values_and_hierarchy(code)


def run_upstream(
    substance_uri: Optional[str],
    material_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    region_code: str,
    include_nondetects: bool = False,
    timeout: Optional[int] = None,
    naics_code: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list, Optional[str]]:
    """
    Run upstream tracing: 3 self-contained federation queries.

    Returns:
        (samples_df, upstream_s2_df, upstream_flowlines_df, facilities_df, executed_queries, error)
        - upstream_s2_df is always empty; Step 2 info is in upstream_flowlines_df.
        - executed_queries: list of dicts with label, endpoint, response_status, row_count, error, query (exact SPARQL run).
    """
    if not (region_code and region_code.strip()):
        return (
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            [],
            "Region (state/county) is required.",
        )

    region_code = region_code.strip()
    subst_filter = sparql_values_uri("substance", substance_uri)
    mat_filter = sparql_values_uri("matType", material_uri)
    region_pattern = region_pattern_sparql(region_code)
    conc_filter = concentration_filter_sparql(min_conc, max_conc, include_nondetects)
    industry_values, industry_hierarchy = _build_upstream_industry_filter(naics_code)

    sample_s2_subquery = f"""
    SELECT DISTINCT ?s2cell WHERE {{
        ?sp rdf:type coso:SamplePoint ;
            spatial:connectedTo ?regionURI ;
            spatial:connectedTo ?s2 .
        {region_pattern} 
        ?s2 rdf:type kwg-ont:S2Cell_Level13 . 
        ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
                kwg-ont:sfTouches | owl:sameAs ?s2 ; 
                spatial:connectedTo ?waterbody .    
        ?waterbody a hyf:HY_WaterBody .            
        ?observation rdf:type coso:ContaminantObservation ; 
                    coso:observedAtSamplePoint ?sp ; 
                    coso:ofDSSToxSubstance ?substance ; 
                    coso:analyzedSample ?sample ; 
                    coso:hasResult ?result .        
        ?sample coso:sampleOfMaterialType ?matType .
        ?result coso:measurementValue ?result_value ;
                coso:measurementUnit ?unit .
        OPTIONAL {{ ?result qudt:quantityValue/qudt:numericValue ?numericResult }}
        OPTIONAL {{ ?result qudt:enumeratedValue ?enumDetected }}
        BIND( (BOUND(?enumDetected) || LCASE(STR(?result_value)) = "non-detect" || STR(?result_value) = STR(coso:non-detect)) as ?isNonDetect )
        BIND( IF(?isNonDetect, 0, COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result_value))) as ?numericValue )
        VALUES ?unit {{ <http://qudt.org/vocab/unit/NanoGM-PER-L> }}
        {subst_filter}
        {mat_filter}
        {conc_filter}
    }} GROUP BY ?s2cell
    """

    executed_queries: list = []
    samples_df = pd.DataFrame()
    upstream_flowlines_df = pd.DataFrame()
    facilities_df = pd.DataFrame()

    # Step 1: PFAS samples (raw per-observation rows)
    q1 = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?samplePoint ?samplePointName ?spWKT ?s2cell
    ?sample ?sampleIdentifier ?date ?substance ?result ?unit ?sampleType
WHERE {{
    ?samplePoint rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        spatial:connectedTo ?regionURI ;
        spatial:connectedTo ?s2 .
    OPTIONAL {{ ?samplePoint rdfs:label ?samplePointName }}
    ?regionURI rdf:type kwg-ont:AdministrativeRegion_3 .
    {region_pattern}
    ?s2 rdf:type kwg-ont:S2Cell_Level13 .
    ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
             kwg-ont:sfTouches | owl:sameAs ?s2 ;
             spatial:connectedTo ?waterbody .
    ?waterbody a hyf:HY_WaterBody .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?samplePoint ;
                coso:ofDSSToxSubstance ?substanceURI ;
                coso:analyzedSample ?sample ;
                coso:hasResult ?resultNode .
    ?substanceURI rdfs:label ?substance .
    ?sample coso:sampleOfMaterialType ?matType .
    OPTIONAL {{ ?sample coso:sampleOfMaterialType/rdfs:label ?sampleType }}
    OPTIONAL {{ ?sample dcterms:identifier ?sampleIdentifier }}
    OPTIONAL {{ ?observation coso:observedTime ?date }}
    ?resultNode coso:measurementValue ?result ;
               coso:measurementUnit ?unitURI .
    ?unitURI qudt:symbol ?unit .
    OPTIONAL {{ ?resultNode qudt:quantityValue/qudt:numericValue ?numericResult }}
    OPTIONAL {{ ?resultNode qudt:enumeratedValue ?enumDetected }}
    BIND( (BOUND(?enumDetected) || LCASE(STR(?result)) = "non-detect" || STR(?result) = STR(coso:non-detect)) as ?isNonDetect )
    BIND( IF(?isNonDetect, 0, COALESCE(xsd:decimal(?numericResult), xsd:decimal(?result))) as ?numericValue )
    VALUES ?unitURI {{ <http://qudt.org/vocab/unit/NanoGM-PER-L> }}
    {subst_filter}
    {mat_filter}
    {conc_filter}
}}
"""
    js1, err1, dbg1 = post_sparql_with_debug("federation", q1, timeout=timeout)
    executed_queries.append(
        build_query_debug_entry(
            "Step 1: PFAS Samples",
            dbg1,
            row_count=len(parse_sparql_results(js1)) if js1 else 0,
            error=err1,
            query=q1,
        )
    )
    if err1:
        return samples_df, pd.DataFrame(), upstream_flowlines_df, facilities_df, executed_queries, err1
    samples_df = parse_sparql_results(js1) if js1 else pd.DataFrame()

    # Step 2: Upstream flowlines
    q2 = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?upstream_flowline ?us_ftype ?upstream_flowlineWKT
WHERE {{
    {{ SELECT DISTINCT ?s2wb WHERE {{
        ?wb a hyf:HY_WaterBody ;
            geo:hasGeometry/geo:asWKT ?wbWKT ;
            spatial:connectedTo ?s2cell ;
            spatial:connectedTo ?s2wb .
        ?s2wb a kwg-ont:S2Cell_Level13 .
        {{ {sample_s2_subquery} }}
    }} }}
    ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                         spatial:connectedTo ?s2wb ;
                         nhdplusv2:hasFTYPE ?ds_ftype .
    ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline ;
                       geo:hasGeometry/geo:asWKT ?upstream_flowlineWKT ;
                       nhdplusv2:hasFTYPE ?us_ftype .
}}
"""
    js2, err2, dbg2 = post_sparql_with_debug("federation", q2, timeout=timeout)
    executed_queries.append(
        build_query_debug_entry(
            "Step 2: Upstream Flowlines",
            dbg2,
            row_count=len(parse_sparql_results(js2)) if js2 else 0,
            error=err2,
            query=q2,
        )
    )
    if err2:
        return samples_df, pd.DataFrame(), upstream_flowlines_df, facilities_df, executed_queries, err2
    upstream_flowlines_df = parse_sparql_results(js2) if js2 else pd.DataFrame()

    # Step 3: Upstream facilities
    if industry_values:
        facility_industry_pattern = f"""
    ?facility fio:ofIndustry ?industryGroup ;
             fio:ofIndustry ?industryCode ;
             geo:hasGeometry/geo:asWKT ?facWKT ;
             rdfs:label ?facilityName .
    ?industryCode a naics:NAICS-IndustryCode ;
                  fio:subcodeOf ?industryGroup ;
                  rdfs:label ?industryName .
    {industry_hierarchy}
    {industry_values}
"""
    else:
        facility_industry_pattern = """
    ?facility fio:ofIndustry ?industryCode ;
             geo:hasGeometry/geo:asWKT ?facWKT ;
             rdfs:label ?facilityName .
    ?industryCode rdfs:label ?industryName .
"""

    q3 = f"""
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>
PREFIX kwg-ont: <http://stko-kwg.geog.ucsb.edu/lod/ontology/>
PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX spatial: <http://purl.org/spatialai/spatial/spatial-full#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?facility ?facWKT ?facilityName ?industryCode ?industryName
WHERE {{
    {{ SELECT DISTINCT ?s2fl WHERE {{
        {{ SELECT DISTINCT ?s2wb WHERE {{
            ?wb a hyf:HY_WaterBody ;
                geo:hasGeometry/geo:asWKT ?wbWKT ;
                spatial:connectedTo ?s2cell ;
                spatial:connectedTo ?s2wb .
            ?s2wb a kwg-ont:S2Cell_Level13 .
            {{ {sample_s2_subquery} }}
        }} }}
        ?downstream_flowline rdf:type hyf:HY_FlowPath ;
                             spatial:connectedTo ?s2wb ;
                             nhdplusv2:hasFTYPE ?ds_ftype .
        ?upstream_flowline hyf:downstreamFlowPathTC ?downstream_flowline ;
                           geo:hasGeometry/geo:asWKT ?upstream_flowlineWKT ;
                           nhdplusv2:hasFTYPE ?us_ftype .
        ?s2fl spatial:connectedTo ?upstream_flowline ;
              rdf:type kwg-ont:S2Cell_Level13 .
    }} }}
    ?s2fl kwg-ont:sfContains ?facility .
    {facility_industry_pattern}
}}
"""
    js3, err3, dbg3 = post_sparql_with_debug("federation", q3, timeout=timeout)
    executed_queries.append(
        build_query_debug_entry(
            "Step 3: Upstream Facilities",
            dbg3,
            row_count=len(parse_sparql_results(js3)) if js3 else 0,
            error=err3,
            query=q3,
        )
    )
    if err3:
        return samples_df, pd.DataFrame(), upstream_flowlines_df, facilities_df, executed_queries, err3
    facilities_df = parse_sparql_results(js3) if js3 else pd.DataFrame()

    upstream_s2_df = pd.DataFrame()
    return samples_df, upstream_s2_df, upstream_flowlines_df, facilities_df, executed_queries, None
