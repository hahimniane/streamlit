"""
Core SPARQL Utilities
Unified module for all SPARQL connection and query operations.
This is the single source of truth for SPARQL utilities.
"""
from __future__ import annotations

from typing import Any, Optional
from datetime import datetime, timezone
import time
import pandas as pd
import rdflib
import requests
from SPARQLWrapper import SPARQLWrapper2, JSON, POST, DIGEST


# =============================================================================
# SPARQL ENDPOINT URLS - Single source of truth
# =============================================================================

ENDPOINT_URLS = {
    'sawgraph': "https://frink.apps.renci.org/sawgraph/sparql",
    'spatial': "https://frink.apps.renci.org/spatialkg/sparql",
    'hydrology': "https://frink.apps.renci.org/hydrologykg/sparql",
    'fio': "https://frink.apps.renci.org/fiokg/sparql",
    'federation': "https://frink.apps.renci.org/federation/sparql"
}

# Alias for backward compatibility
ENDPOINTS = ENDPOINT_URLS


# =============================================================================
# SPARQL WRAPPER FUNCTIONS
# =============================================================================

def get_sparql_wrapper(endpoint_name: str) -> SPARQLWrapper2:
    """
    Create and configure a SPARQLWrapper instance for the specified endpoint.
    
    Args:
        endpoint_name: Key from ENDPOINT_URLS dict ('sawgraph', 'spatial', 'hydrology', 'fio', 'federation')
    
    Returns:
        Configured SPARQLWrapper2 instance
    
    Raises:
        ValueError: If endpoint_name is not recognized
    """
    if endpoint_name not in ENDPOINT_URLS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}. Choose from {list(ENDPOINT_URLS.keys())}")
    
    sparql = SPARQLWrapper2(ENDPOINT_URLS[endpoint_name])
    sparql.setHTTPAuth(DIGEST)
    sparql.setMethod(POST)
    sparql.setReturnFormat(JSON)
    return sparql


# =============================================================================
# RESULT PARSING FUNCTIONS
# =============================================================================

def parse_sparql_results(results: dict) -> pd.DataFrame:
    """
    Convert SPARQL JSON results to pandas DataFrame.
    
    This is THE canonical function for parsing SPARQL results.
    All other modules should import this function from here.
    
    Args:
        results: SPARQL JSON response with 'head' and 'results' keys
    
    Returns:
        pandas DataFrame with one row per binding
    """
    if not results or 'results' not in results or 'head' not in results:
        return pd.DataFrame()
    
    variables = results['head']['vars']
    bindings = results['results']['bindings']
    
    if not bindings:
        return pd.DataFrame(columns=variables)
    
    data = []
    for binding in bindings:
        row = {}
        for var in variables:
            if var in binding:
                row[var] = binding[var]['value']
            else:
                row[var] = None
        data.append(row)
    
    return pd.DataFrame(data)


def convertToDataframe(_results) -> pd.DataFrame:
    """
    Convert SPARQLWrapper2 results to pandas DataFrame.
    
    This function handles the SPARQLWrapper2 result format (with .bindings attribute).
    For JSON results from requests, use parse_sparql_results() instead.
    
    Args:
        _results: SPARQLWrapper2 query results object
    
    Returns:
        pandas DataFrame
    """
    d = []
    for x in _results.bindings:
        row = {}
        for k in x:
            v = x[k]
            vv = rdflib.term.Literal(v.value, datatype=v.datatype).toPython()
            row[k] = vv
        d.append(row)
    df = pd.DataFrame(d)
    return df


# =============================================================================
# QUERY BUILDING HELPERS
# =============================================================================

def sparql_values_uri(var_name: str, uri: Optional[str]) -> str:
    """
    Build a SPARQL VALUES clause for a single URI variable (e.g. ?substance, ?matType).

    Args:
        var_name: Variable name without '?' (e.g. 'substance', 'matType').
        uri: Full URI string; if None or empty, returns empty string.

    Returns:
        "VALUES ?varName { <uri> }" or "".
    """
    if not uri or not uri.strip():
        return ""
    u = uri.strip()
    if u.startswith("<") and u.endswith(">"):
        return f"VALUES ?{var_name} {{ {u} }}"
    return f"VALUES ?{var_name} {{ <{u}> }}"


def region_pattern_sparql(region_code: str) -> str:
    """
    Build the SPARQL graph pattern for filtering by US state or county.

    Uses ?regionURI. For long codes (e.g. FIPS county), binds single region URI.
    For short codes (state), uses type and administrativePartOf+.

    Args:
        region_code: State FIPS (e.g. '23') or full FIPS (e.g. state+county).

    Returns:
        SPARQL fragment to inject into WHERE (no outer braces).
    """
    code = (region_code or "").strip()
    if not code:
        return ""
    if len(code) > 5:
        return f"VALUES ?regionURI {{ <https://datacommons.org/browser/geoId/{code}> }}"
    return (
        f"?regionURI rdf:type kwg-ont:AdministrativeRegion_3 ; "
        f"kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{code} ."
    )


def concentration_filter_sparql(
    min_conc: float,
    max_conc: float,
    include_nondetects: bool,
) -> str:
    """
    Build SPARQL FILTER clauses for concentration (assumes ?numericValue and ?isNonDetect).

    Args:
        min_conc: Minimum concentration (inclusive).
        max_conc: Maximum concentration (inclusive).
        include_nondetects: If True, allow non-detects and filter numeric range.

    Returns:
        Newline-joined FILTER lines.
    """
    if include_nondetects:
        return (
            f"FILTER( ?isNonDetect || (BOUND(?numericValue) && ?numericValue >= {min_conc} && ?numericValue <= {max_conc}) )"
        )
    return "\n".join([
        "FILTER(!?isNonDetect)",
        "FILTER(BOUND(?numericValue))",
        "FILTER(?numericValue > 0)",
        f"FILTER (?numericValue >= {min_conc})",
        f"FILTER (?numericValue <= {max_conc})",
    ])


def convert_s2_list_to_query_string(s2_list: list[str]) -> str:
    """
    Convert S2 cell URIs to SPARQL VALUES clause format.

    S2 cells are identified by full URIs (e.g. from the knowledge graph).
    For SPARQL queries using PREFIX kwgr: <http://stko-kwg.geog.ucsb.edu/lod/resource/>,
    this produces compact values like "kwgr:s2cell_level13_12345".

    Use when building VALUES clauses for S2 cell lists (e.g. in upstream/downstream
    tracing analyses).

    Args:
        s2_list: List of S2 cell URIs or prefixed identifiers (strings).

    Returns:
        Space-separated S2 cell identifiers for use in a SPARQL VALUES clause.
    """
    formatted = []
    for s2 in s2_list:
        if s2.startswith("http://stko-kwg.geog.ucsb.edu/lod/resource/"):
            formatted.append(s2.replace("http://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:"))
        elif s2.startswith("https://stko-kwg.geog.ucsb.edu/lod/resource/"):
            formatted.append(s2.replace("https://stko-kwg.geog.ucsb.edu/lod/resource/", "kwgr:"))
        elif s2.startswith("kwgr:"):
            formatted.append(s2)
        elif s2.startswith("http://") or s2.startswith("https://"):
            formatted.append(f"<{s2}>")
        else:
            formatted.append(s2)
    return " ".join(formatted)


def state_code_from_region(region_code: Optional[str]) -> Optional[str]:
    """
    Extract the 2-digit state code from a region code.

    Args:
        region_code: FIPS region code (state or county).
            - 2-digit: returned as-is (state code)
            - 5-digit: first 2 digits returned (state from county)
            - Other: None

    Returns:
        2-digit state code or None if not extractable.
    """
    if not region_code:
        return None
    code = str(region_code).strip()
    if not code:
        return None
    if len(code) == 5:
        return code[:2]
    if len(code) <= 2:
        return code
    return None


def build_county_region_filter(
    region_code: Optional[str],
    county_var: str = "?county",
) -> str:
    """
    Build a SPARQL pattern to filter by region (state or county).

    Used when filtering facilities by the administrative region they're connected to.

    Args:
        region_code: FIPS code - either 2-digit state (e.g. "23") or 5-digit county (e.g. "23011").
        county_var: SPARQL variable name for the region (default: ?county).

    Returns:
        SPARQL fragment or empty string if no valid code.
        - 2-digit: filters counties (AR2) within the state
        - 5-digit: filters subdivisions (AR3) within the specific county
    """
    if not region_code:
        return ""
    code = str(region_code).strip()
    if not code:
        return ""
    if len(code) == 5:
        # County code: use AdministrativeRegion_3 with transitive administrativePartOf+
        return (
            f"{county_var} rdf:type kwg-ont:AdministrativeRegion_3 ;\n"
            f"               kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{code} ."
        )
    if len(code) == 2:
        # State code: use AdministrativeRegion_2
        return (
            f"{county_var} rdf:type kwg-ont:AdministrativeRegion_2 ;\n"
            f"               kwg-ont:administrativePartOf kwgr:administrativeRegion.USA.{code} ."
        )
    return ""


def build_ar3_region_filter(
    region_code: Optional[str],
    ar3_var: str = "?ar3",
) -> str:
    """
    Build a SPARQL pattern to filter by AR3 administrative regions.

    AR3 regions are finer-grained administrative units (e.g. subdivisions).

    Args:
        region_code: FIPS region code.
            - >5 digits: binds ar3_var to exact geoId URI
            - <=5 digits: uses administrativePartOf+ to find AR3s within region
        ar3_var: SPARQL variable name for the AR3 region (default: ?ar3).

    Returns:
        SPARQL fragment or empty string if no valid code.
    """
    if not region_code:
        return ""
    code = str(region_code).strip()
    if not code:
        return ""
    if len(code) > 5:
        return f"VALUES {ar3_var} {{ <https://datacommons.org/browser/geoId/{code}> }} ."
    return (
        f"{ar3_var} rdf:type kwg-ont:AdministrativeRegion_3 ; "
        f"kwg-ont:administrativePartOf+ kwgr:administrativeRegion.USA.{code} ."
    )


def build_facility_values(facility_uris: Optional[list[str]]) -> str:
    """
    Build a SPARQL VALUES clause for a list of facility URIs.

    Handles various URI formats (bare URIs, angle-bracketed, http/https).

    Args:
        facility_uris: List of facility URI strings.

    Returns:
        SPARQL VALUES clause like "VALUES ?facility { <uri1> <uri2> }."
        or empty string if no valid URIs.
    """
    if not facility_uris:
        return ""
    cleaned: list[str] = []
    for uri in facility_uris:
        if not uri:
            continue
        u = str(uri).strip()
        if not u:
            continue
        if u.startswith("<") and u.endswith(">"):
            cleaned.append(u)
        elif u.startswith("http://") or u.startswith("https://"):
            cleaned.append(f"<{u}>")
    if not cleaned:
        return ""
    return f"VALUES ?facility {{ {' '.join(cleaned)} }}."


# =============================================================================
# QUERY EXECUTION FUNCTIONS
# =============================================================================

def post_sparql_with_debug(
    endpoint_key: str,
    query: str,
    timeout: Optional[int] = None,
) -> tuple[Optional[dict], Optional[str], dict]:
    """
    POST a SPARQL query to a known endpoint and return (json, error, debug_info).

    Args:
        endpoint_key: Key from ENDPOINT_URLS (e.g. 'federation').
        query: SPARQL query string.
        timeout: Request timeout in seconds.

    Returns:
        (json_response, error_message, debug_dict). debug_dict has endpoint, query,
        response_status, and optionally exception.
    """
    started_perf = time.perf_counter()
    started_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _elapsed_ms() -> float:
        return (time.perf_counter() - started_perf) * 1000.0

    if endpoint_key not in ENDPOINT_URLS:
        return None, f"Unknown endpoint: {endpoint_key}", {
            "endpoint": None,
            "query": query,
            "timeout_sec": timeout,
            "started_at_utc": started_at_utc,
            "elapsed_ms": _elapsed_ms(),
        }
    endpoint = ENDPOINT_URLS[endpoint_key]
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    debug: dict[str, Any] = {
        "endpoint": endpoint,
        "query": query,
        "timeout_sec": timeout,
        "started_at_utc": started_at_utc,
    }
    try:
        response = requests.post(
            endpoint, data={"query": query}, headers=headers, timeout=timeout
        )
        debug["elapsed_ms"] = _elapsed_ms()
        debug["response_status"] = response.status_code
        if response.status_code != 200:
            return (
                None,
                f"Error {response.status_code}: {response.text[:500]}",
                debug,
            )
        return response.json(), None, debug
    except requests.exceptions.RequestException as e:
        debug["elapsed_ms"] = _elapsed_ms()
        debug["exception"] = str(e)
        return None, f"Network error: {str(e)}", debug
    except Exception as e:
        debug["elapsed_ms"] = _elapsed_ms()
        debug["exception"] = str(e)
        return None, f"Error: {str(e)}", debug


def build_query_debug_entry(
    label: str,
    debug_info: Optional[dict[str, Any]],
    row_count: Optional[int] = None,
    error: Optional[str] = None,
    query: Optional[str] = None,
) -> dict[str, Any]:
    """
    Normalize a per-step query debug record for UI rendering and telemetry.
    """
    debug = debug_info or {}
    return {
        "label": label,
        "endpoint": debug.get("endpoint"),
        "timeout_sec": debug.get("timeout_sec"),
        "response_status": debug.get("response_status"),
        "elapsed_ms": debug.get("elapsed_ms"),
        "row_count": row_count,
        "error": error or debug.get("exception"),
        "query": query if query is not None else debug.get("query"),
    }


def execute_sparql_query(
    endpoint: str,
    query: str,
    method: str = 'POST',
    timeout: Optional[int] = None
) -> Optional[dict]:
    """
    Execute a SPARQL query and return JSON results.
    
    This is THE canonical function for executing SPARQL queries via HTTP.
    
    Args:
        endpoint: Full URL of the SPARQL endpoint, or key from ENDPOINT_URLS
        query: SPARQL query string
        method: HTTP method ('POST' or 'GET')
        timeout: Request timeout in seconds (None = no timeout)
    
    Returns:
        JSON response dict, or None if query failed
    """
    # Allow passing endpoint name instead of full URL
    if endpoint in ENDPOINT_URLS:
        endpoint = ENDPOINT_URLS[endpoint]
    
    headers = {
        'Accept': 'application/sparql-results+json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        if method.upper() == 'POST':
            response = requests.post(endpoint, data={'query': query}, headers=headers, timeout=timeout)
        else:
            response = requests.get(endpoint, params={'query': query}, headers=headers, timeout=timeout)
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"SPARQL query error: {e}")
        return None


def test_connection(endpoint_name: str = 'sawgraph') -> tuple[bool, str, Optional[pd.DataFrame]]:
    """
    Test connection to a SPARQL endpoint.
    
    Args:
        endpoint_name: Endpoint to test
    
    Returns:
        tuple: (success: bool, message: str, data: DataFrame or None)
    """
    try:
        test_query = '''
PREFIX coso: <http://w3id.org/coso/v1/contaminoso#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?substance ?substanceLabel (COUNT(?obs) as ?count) WHERE {
    ?obs rdf:type coso:ContaminantObservation;
         coso:ofSubstance ?substance;
         coso:hasResult ?result.
    ?substance rdfs:label ?substanceLabel.
    ?result coso:measurementValue ?value.
    FILTER(?value > 0)
} GROUP BY ?substance ?substanceLabel
ORDER BY DESC(?count)
LIMIT 10
'''
        sparql = get_sparql_wrapper(endpoint_name)
        sparql.setQuery(test_query)
        result = sparql.query()
        df = convertToDataframe(result)
        return True, f"Connected to {endpoint_name} successfully!", df
    except Exception as e:
        return False, f"Connection failed: {str(e)}", None
