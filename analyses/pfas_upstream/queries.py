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
    concentration_filter_sparql,
    parse_sparql_results,
    post_sparql_with_debug,
    region_pattern_sparql,
    sparql_values_uri,
)


def run_upstream(
    substance_uri: Optional[str],
    material_uri: Optional[str],
    min_conc: float,
    max_conc: float,
    region_code: str,
    include_nondetects: bool = False,
    timeout: Optional[int] = None,
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
                    coso:ofSubstance ?substance ;
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

    # Step 1: Contaminated samples
    q1 = f"""
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
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT (COUNT(DISTINCT ?subVal) as ?resultCount) (MAX(?result_value) as ?maxResultValue) (SAMPLE(?substance) as ?substanceSample) (SAMPLE(?matType) as ?matTypeSample) (SAMPLE(?regionURI) as ?regionURISample) ?sp ?spWKT ?s2cell WHERE {{
    ?sp rdf:type coso:SamplePoint ;
        geo:hasGeometry/geo:asWKT ?spWKT ;
        spatial:connectedTo ?regionURI ;
        spatial:connectedTo ?s2 .
    ?regionURI rdf:type kwg-ont:AdministrativeRegion_3 .
    {region_pattern}
    ?s2 rdf:type kwg-ont:S2Cell_Level13 .
    ?s2cell rdf:type kwg-ont:S2Cell_Level13 ;
             kwg-ont:sfTouches | owl:sameAs ?s2 ;
             spatial:connectedTo ?waterbody .
    ?waterbody a hyf:HY_WaterBody .
    ?observation rdf:type coso:ContaminantObservation ;
                coso:observedAtSamplePoint ?sp ;
                coso:ofSubstance ?substance ;
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
    BIND(CONCAT(STR(?result_value), " ", "ng/L") as ?subVal)
}} GROUP BY ?sp ?spWKT ?s2cell
"""
    js1, err1, dbg1 = post_sparql_with_debug("federation", q1, timeout=timeout)
    executed_queries.append({
        "label": "Step 1: PFAS Samples",
        "endpoint": dbg1.get("endpoint"),
        "response_status": dbg1.get("response_status"),
        "row_count": len(parse_sparql_results(js1)) if js1 else 0,
        "error": err1 or dbg1.get("exception"),
        "query": q1,
    })
    if err1:
        return samples_df, pd.DataFrame(), upstream_flowlines_df, facilities_df, executed_queries, err1
    samples_df = parse_sparql_results(js1) if js1 else pd.DataFrame()
    if not samples_df.empty:
        renames = {}
        if "maxResultValue" in samples_df.columns:
            renames["maxResultValue"] = "result_value"
        if "substanceSample" in samples_df.columns:
            renames["substanceSample"] = "substance"
        if "matTypeSample" in samples_df.columns:
            renames["matTypeSample"] = "matType"
        if "regionURISample" in samples_df.columns:
            renames["regionURISample"] = "regionURI"
        if renames:
            samples_df = samples_df.rename(columns=renames)

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
    executed_queries.append({
        "label": "Step 2: Upstream Flowlines",
        "endpoint": dbg2.get("endpoint"),
        "response_status": dbg2.get("response_status"),
        "row_count": len(parse_sparql_results(js2)) if js2 else 0,
        "error": err2 or dbg2.get("exception"),
        "query": q2,
    })
    if err2:
        return samples_df, pd.DataFrame(), upstream_flowlines_df, facilities_df, executed_queries, err2
    upstream_flowlines_df = parse_sparql_results(js2) if js2 else pd.DataFrame()

    # Step 3: Upstream facilities
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
    ?facility fio:ofIndustry ?industryCode ;
             geo:hasGeometry/geo:asWKT ?facWKT ;
             rdfs:label ?facilityName .
    ?industryCode rdfs:label ?industryName .
}}
"""
    js3, err3, dbg3 = post_sparql_with_debug("federation", q3, timeout=timeout)
    executed_queries.append({
        "label": "Step 3: Upstream Facilities",
        "endpoint": dbg3.get("endpoint"),
        "response_status": dbg3.get("response_status"),
        "row_count": len(parse_sparql_results(js3)) if js3 else 0,
        "error": err3 or dbg3.get("exception"),
        "query": q3,
    })
    if err3:
        return samples_df, pd.DataFrame(), upstream_flowlines_df, facilities_df, executed_queries, err3
    facilities_df = parse_sparql_results(js3) if js3 else pd.DataFrame()

    upstream_s2_df = pd.DataFrame()
    return samples_df, upstream_s2_df, upstream_flowlines_df, facilities_df, executed_queries, None
