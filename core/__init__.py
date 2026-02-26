"""
Core Module
Provides generic utilities for SPARQL operations and data loading.
"""
from core.sparql import (
    ENDPOINT_URLS,
    ENDPOINTS,
    build_query_debug_entry,
    concentration_filter_sparql,
    convert_s2_list_to_query_string,
    convertToDataframe,
    execute_sparql_query,
    get_sparql_wrapper,
    parse_sparql_results,
    post_sparql_with_debug,
    region_pattern_sparql,
    sparql_values_uri,
    test_connection,
)

from core.data_loader import (
    load_fips_data,
    load_substances_data,
    load_material_types_data,
    parse_regions,
    load_all_data,
    build_substance_options,
    build_material_type_options,
)

__all__ = [
    # SPARQL
    "ENDPOINT_URLS",
    "ENDPOINTS",
    "build_query_debug_entry",
    "concentration_filter_sparql",
    "convert_s2_list_to_query_string",
    "convertToDataframe",
    "execute_sparql_query",
    "get_sparql_wrapper",
    "parse_sparql_results",
    "post_sparql_with_debug",
    "region_pattern_sparql",
    "sparql_values_uri",
    "test_connection",
    # Data Loading
    "load_fips_data",
    "load_substances_data",
    "load_material_types_data",
    "parse_regions",
    "load_all_data",
    "build_substance_options",
    "build_material_type_options",
]
