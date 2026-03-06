# SAWGraph PFAS Explorer - AI Model Guidelines

This file is the authoritative guide for AI models creating or modifying analyses in this codebase. Follow every section exactly. Do not deviate from established patterns without explicit instruction.

---

## Project Overview

A Streamlit app for exploring PFAS contamination via the SAWGraph knowledge graph. It uses a modular analysis architecture where each analysis lives in its own folder, shares common utilities via `core/`, `filters/`, and `components/`, and is registered in `analysis_registry.py`.

---

## Non-Negotiable Rules

1. **Never** import from `.venv/` or reference vendored packages directly.
2. **Always** use `from __future__ import annotations` at the top of every Python file.
3. **Always** use `post_sparql_with_debug` from `core.sparql` for all SPARQL queries — never raw `requests` or `SPARQLWrapper` directly in analysis code.
4. **Never** duplicate SPARQL helper logic already in `core/sparql.py` (region filters, concentration filters, NAICS utils).
5. **Never** use `st.session_state` directly in an analysis — always use `AnalysisState`.
6. **Always** register a new analysis in `analysis_registry.py` before it is considered complete.
7. **Never** create standalone scripts or one-off utilities unless explicitly asked.
8. **Never** add a `queries.py` entry point other than named functions that return `(df, error, debug_info)` or the upstream tuple shape described below.

---

## Creating a New Analysis — Checklist

### Step 1: Create the folder

```
analyses/my_analysis/
    __init__.py   # empty
    analysis.py   # UI + orchestration
    queries.py    # SPARQL query functions only
```

### Step 2: Write `queries.py`

- Module docstring describing what queries are in the file.
- Import only from `core.sparql`, `core.naics_utils`, `pandas`, and stdlib.
- Each public query function returns a 3-tuple: `(pd.DataFrame, Optional[str], Dict[str, Any])` — data, error string or None, debug dict.
  - Exception: the upstream pattern returns `(df1, df2, df3, df4, list, Optional[str])` — follow that shape only if the analysis genuinely needs it.
- The debug dict must always contain at minimum `"label"`, `"error"`, and `"row_count"` keys, populated via `build_query_debug_entry` from `core.sparql`.
- All queries go to the `"federation"` endpoint unless there is a specific reason to use another.
- Always declare all SPARQL prefixes at the top of every query string.
- Use helper functions from `core.sparql` for repetitive fragments:
  - `region_pattern_sparql(region_code)` — region membership pattern
  - `concentration_filter_sparql(min_conc, max_conc, include_nondetects)` — concentration FILTER block
  - `sparql_values_uri(var, uri)` — VALUES clause for optional URI filter
  - `build_county_region_filter(region_code)` — facility county region filter
  - `build_facility_values(facilities)` — VALUES clause from a list of facility URIs
  - `post_sparql_with_debug(endpoint_name, query, timeout=None)` — execute and return `(json, error, debug_info)`
  - `parse_sparql_results(json)` — parse response to DataFrame
- Use NAICS helpers from `core.naics_utils`:
  - `normalize_naics_codes(naics_code)` — normalize input to a list of clean strings
  - `build_naics_values_and_hierarchy(code)` — returns `(industry_values, industry_hierarchy)` SPARQL fragments

### Step 3: Write `analysis.py`

Follow this exact structure inside `main(context: AnalysisContext) -> None`:

```python
"""
My Analysis Title (Query N)
One-line description.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
from streamlit_folium import st_folium

from analysis_registry import AnalysisContext
from analyses.my_analysis.queries import my_query_fn

# Filters (use only what is needed)
from filters.industry import render_sidebar_industry_selector
from filters.concentration import render_concentration_filter, apply_concentration_filter
from filters.substance import get_cached_substances_with_labels
from filters.material import get_cached_material_types_with_labels

# Shared components (import only what is used)
from core.boundary import fetch_boundaries
from core.geometry import create_geodataframe
from core.sparql import build_query_debug_entry
from components.parameter_display import (
    build_concentration_params, build_industry_params,
    build_region_params, render_parameter_table,
)
from components.result_display import render_step_results, render_metrics_row, render_data_expander
from components.map_rendering import (
    FACILITY_MARKER_RADIUS,
    add_naics_link_column, add_naics_url_column, add_facility_link_column,
    create_base_map, add_boundary_layers, add_point_layer,
    add_line_layer, add_grouped_point_layers, finalize_map, render_map_legend,
)
from components.execute_button import render_execute_button, check_required_fields
from components.analysis_state import AnalysisState, check_old_session_keys
from components.step_execution import StepExecutor
from components.query_debug import render_executed_queries
from components.eta_display import render_simple_eta
from core.runtime_eta import (
    build_eta_request, estimate_eta,
    naics_prefix2_from_code, record_executed_query_batch,
)


def main(context: AnalysisContext) -> None:
    """Main function for My Analysis"""
    # 1. Migrate old session keys (list any keys from previous versions)
    check_old_session_keys(['old_key_one', 'old_key_two'])

    # 2. Description block
    st.markdown("""
    **What this analysis does:**
    - ...

    **N-Step Process:** Step A -> Step B -> ...
    """)

    # 3. Initialize state
    state = AnalysisState(context.analysis_key)
    state.init_if_missing('executed_queries', [])

    # 4. Sidebar parameters
    st.sidebar.markdown("### Query Parameters")
    # ... add filters here ...

    # 5. Execute button
    execute_clicked = render_execute_button(
        help_text="Execute the analysis"
    )

    # 6. ETA preview (always before execution block)
    preview_request = build_eta_request(
        analysis_key=context.analysis_key,
        region_code=context.region_code,
        state_code=context.selected_state_code,
        min_conc=conc_filter.min_concentration,
        max_conc=conc_filter.max_concentration,
        include_nondetects=conc_filter.include_nondetects,
        naics_prefix2=naics_prefix2_from_code(selected_naics_code),
        has_substance_filter=False,
        has_material_filter=False,
    )
    render_simple_eta(estimate_eta(preview_request))

    # 7. Query execution
    if execute_clicked:
        st.markdown("---")
        st.subheader("Query Execution")

        run_request = build_eta_request(...)
        run_eta = estimate_eta(run_request)

        executor = StepExecutor(num_steps=N)
        result_df = pd.DataFrame()
        executed_queries = []
        step_eta_by_label = {s.label: s for s in run_eta.step_estimates}

        with executor.step(1, "Doing step 1...") as step:
            result_df, error, debug = my_query_fn(...)
            step_info = build_query_debug_entry("Step 1: Label", debug,
                row_count=len(result_df), error=error)
            executed_queries.append(step_info)
            if error:
                step.error(f"Step 1 failed: {error}")
            elif not result_df.empty:
                step.success(f"Step 1: Found {len(result_df)} results")
            else:
                step.warning("Step 1: No results found")

        record_executed_query_batch(
            request=run_request,
            executed_queries=executed_queries,
            step_eta_by_label=step_eta_by_label,
        )
        state.set("executed_queries", executed_queries)
        state.set_results({
            "result_df": result_df,
            "params_data": [...],
            "query_region_code": context.region_code,
            "executed_queries": executed_queries,
        })

    # 8. Always render debug panel
    render_executed_queries(state.get("executed_queries", []))

    # 9. Display results
    if state.has_results:
        results = state.get_results()
        result_df = results.get("result_df", pd.DataFrame())
        params_data = results.get("params_data", [])
        query_region_code = results.get("query_region_code")

        st.markdown("---")
        render_parameter_table(params_data)
        st.markdown("### Query Results")
        st.markdown("---")

        if not result_df.empty:
            render_step_results("Step 1: Label", result_df, [...metrics...], "View Data",
                download_filename=f"my_analysis_{query_region_code or 'all'}.csv",
                download_key=f"download_{context.analysis_key}_results",
            )

        _render_map(result_df, ...)
    else:
        st.info("Set parameters in the sidebar and click 'Execute Query' to run the analysis")


def _render_map(...) -> None:
    """Render the interactive map."""
    # Guard: return early if no mappable data
    if result_df.empty or 'wktColumn' not in result_df.columns:
        return

    st.markdown("---")
    st.markdown("### Interactive Map")

    gdf = create_geodataframe(result_df, 'wktColumn')
    if gdf is None or gdf.empty:
        return

    map_obj = create_base_map(gdf_list=[gdf], zoom=8)
    add_boundary_layers(map_obj, boundaries, context.region_code)

    # Add layers ...
    add_point_layer(map_obj, gdf, '<span style="color:DarkOrange;">Label</span>',
                    'DarkOrange', popup_fields=[...], radius=6)

    finalize_map(map_obj)
    st_folium(map_obj, width=None, height=600, returned_objects=[])
    render_map_legend([
        "**Orange circles** = ...",
        "**Boundary outline** = Selected region",
    ])
```

### Step 4: Register in `analysis_registry.py`

Add an `AnalysisSpec` to the `specs` list inside `build_registry()`. Also add the lazy import at the top of that function:

```python
from analyses.my_analysis.analysis import main as my_analysis_main
```

```python
AnalysisSpec(
    key="my_analysis",             # unique snake_case key
    label="My Analysis Label",     # shown in sidebar dropdown
    title="Icon My Analysis Title",# shown as page heading (include emoji)
    description="One sentence description shown under the title.",
    query=7,                       # next available query number
    enabled=True,
    runner=my_analysis_main,
    region_config=RegionConfig(
        state="optional",          # "required" | "optional" | "hidden"
        county="optional",
        subdivision="optional",
        availability_source="pfas", # "pfas" | "sockg" | None
    ),
),
```

---

## Key Design Patterns

### `AnalysisContext` Fields

| Field | Type | Description |
|---|---|---|
| `analysis_key` | `str` | Unique key, e.g. `"upstream"` |
| `region_code` | `str` | FIPS code, e.g. `"23"`, `"23005"` |
| `region_display` | `str` | Human label, e.g. `"Maine"` |
| `selected_state_code` | `Optional[str]` | 2-digit FIPS or None |
| `selected_state_name` | `Optional[str]` | State name or None |
| `selected_county_code` | `Optional[str]` | 5-digit FIPS or None |
| `selected_county_name` | `Optional[str]` | County name or None |
| `selected_subdivision_code` | `Optional[str]` | Subdivision code or None |
| `selected_subdivision_name` | `Optional[str]` | Subdivision name or None |
| `endpoints` | `dict` | `ENDPOINT_URLS` dict |
| `project_dir` | `str` | Absolute path to project root |
| `query_number` | `int` | Numeric query index |
| `substances_df` | `pd.DataFrame` | Static PFAS substances data |
| `material_types_df` | `pd.DataFrame` | Static material types data |

### Session State via `AnalysisState`

```python
state = AnalysisState(context.analysis_key)

state.init_if_missing('my_key', default_value)   # safe init
state.set('my_key', value)                        # write
state.get('my_key', default)                      # read

state.set_results({...})   # store query results
state.has_results          # bool — check before reading
state.get_results()        # retrieve stored dict
state.clear_results()      # wipe results
```

**Never** read or write `st.session_state` directly in analysis code.

### SPARQL Query Function Signature

```python
def execute_my_query(
    naics_code: Optional[str],
    region_code: Optional[str],
    min_concentration: float = 0.0,
    max_concentration: float = 500.0,
    include_nondetects: bool = False,
) -> Tuple[pd.DataFrame, Optional[str], Dict[str, Any]]:
    """Step N: description."""
    ...
    results_json, error, debug_info = post_sparql_with_debug("federation", query)
    df = parse_sparql_results(results_json) if results_json else pd.DataFrame()
    debug_info.update({"label": "Step N: Label", "error": error, "row_count": len(df)})
    return df, error, debug_info
```

### ETA Pattern

Always build two requests: one for preview (using filter values before execution) and one for the actual run (using applied values after `apply_concentration_filter`).

```python
# Before execute button — preview ETA
preview_request = build_eta_request(analysis_key=..., region_code=..., state_code=...,
    min_conc=..., max_conc=..., include_nondetects=...,
    naics_prefix2=..., has_substance_filter=..., has_material_filter=...)
render_simple_eta(estimate_eta(preview_request))

# After execute clicked — record actual timing
run_request = build_eta_request(...)
run_eta = estimate_eta(run_request)
step_eta_by_label = {s.label: s for s in run_eta.step_estimates}
# ... run queries ...
record_executed_query_batch(request=run_request, executed_queries=executed_queries,
    step_eta_by_label=step_eta_by_label)
```

### Parameters Table

`params_data` is a list of `{"Parameter": str, "Value": str}` dicts. Use builder helpers:

```python
from components.parameter_display import (
    build_concentration_params,   # returns one dict row
    build_industry_params,        # returns one dict row
    build_region_params,          # returns one dict row
    render_parameter_table,       # renders the full table
)
```

### Metrics and Data Display

```python
# Option A: combined header + metrics + expander
render_step_results(
    header="Step 1: Facilities",
    df=facilities_df,
    metrics=[{"label": "Total Facilities", "value": len(facilities_df)}],
    expander_label="View Facilities Data",
    display_columns=['col1', 'col2'],
    download_filename=f"my_download_{query_region_code}.csv",
    download_key=f"download_{context.analysis_key}_facilities",
    column_config={...},   # optional st.column_config overrides
    show_stats=False,
    stats_column=None,
)

# Option B: separate components
render_metrics_row([{"label": "Count", "value": 42}], num_columns=2)
render_data_expander("View Data", df, display_columns=[...],
    download_filename=..., download_key=..., show_stats=True, stats_column='max')
```

### Map Rendering

```python
map_obj = create_base_map(gdf_list=[gdf1, gdf2], zoom=8)
add_boundary_layers(map_obj, boundaries, context.region_code)

# Point layers
add_point_layer(map_obj, gdf, name='<span style="color:DarkOrange;">Label</span>',
    color='DarkOrange', popup_fields=['col1', 'col2'], radius=6)

# Grouped point layers (one layer per category value)
add_grouped_point_layers(map_obj, gdf, group_col='industryName',
    popup_fields=['facilityName', 'industryName'], radius=FACILITY_MARKER_RADIUS)

# Line layers
add_line_layer(map_obj, gdf, name='<span style="color:DodgerBlue;">Lines</span>',
    color='DodgerBlue', weight=3, opacity=0.5)

finalize_map(map_obj)
st_folium(map_obj, width=None, height=600, returned_objects=[])
render_map_legend([
    "**Color label** = description",
])
```

- Always pass `returned_objects=[]` to `st_folium` to avoid unnecessary re-renders.
- Use HTML `<span style="color:...">` in layer names for legend colors.
- Add NAICS links with `add_naics_link_column(gdf)` (popup HTML) or `add_naics_url_column(df)` (URL column for `st.column_config.LinkColumn`).
- Add facility links with `add_facility_link_column(gdf)`.

### Industry (NAICS) Filter

```python
selected_naics_code, selected_industry_display = render_sidebar_industry_selector(
    analysis_key=context.analysis_key,
    heading="### Industry Type",
    caption="_Optional: leave empty to search all industries_",
    allow_empty=True,
    empty_label="All Industries",  # or "Not Selected" if required
)
```

In `queries.py`, always normalize before use:

```python
from core.naics_utils import normalize_naics_codes, build_naics_values_and_hierarchy

codes = normalize_naics_codes(naics_code)
if not codes:
    industry_values, industry_hierarchy = "", ""
else:
    industry_values, industry_hierarchy = build_naics_values_and_hierarchy(codes[0])
```

### Concentration Filter

```python
conc_filter = render_concentration_filter(context.analysis_key, default_max=500)
# conc_filter.min_concentration, conc_filter.max_concentration, conc_filter.include_nondetects

# Inside execute_clicked block:
min_conc, max_conc, include_nondetects = apply_concentration_filter(context.analysis_key)
```

### Substance and Material Type Filters

```python
is_subdivision = len(context.region_code) > 5 if context.region_code else False

substances_view = (
    get_cached_substances_with_labels(context.region_code, is_subdivision)
    if context.region_code else pd.DataFrame()
)
material_types_view = (
    get_cached_material_types_with_labels(context.region_code, is_subdivision)
    if context.region_code else pd.DataFrame()
)
```

### Boundaries

```python
from core.boundary import fetch_boundaries

boundaries = fetch_boundaries(context.selected_state_code, context.selected_county_code)
# Pass to add_boundary_layers(map_obj, boundaries, context.region_code)
```

---

## SPARQL Endpoints

| Key | Description |
|---|---|
| `federation` | Default — federated cross-graph queries (use this unless there is a reason not to) |
| `sawgraph` | PFAS contamination observations only |
| `spatial` | Administrative boundaries and spatial relationships |
| `hydrology` | NHDPlus V2 water flow networks |
| `fio` | Industrial facilities and NAICS data |

Always pass the endpoint key string (not the URL) to `post_sparql_with_debug`.

---

## SPARQL Prefix Reference

Include all needed prefixes at the top of every query string. Common ones:

```sparql
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
PREFIX fio: <http://w3id.org/fio/v1/fio#>
PREFIX naics: <http://w3id.org/fio/v1/naics#>
PREFIX nhdplusv2: <http://nhdplusv2.spatialai.org/v1/nhdplusv2#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
```

---

## File Naming and Code Style

- File names: `snake_case.py`
- Functions: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private helpers in `analysis.py` and `queries.py`: prefix with `_` (e.g., `_render_map`, `_build_industry_filter`)
- Every file starts with a module docstring immediately after the `from __future__ import annotations` (or before it), then imports.
- Do not add type annotations or docstrings to code you did not write or change.
- Do not add comments unless the logic is non-obvious.

---

## What NOT to Do

- Do not put UI code (`st.*` calls) inside `queries.py`.
- Do not put SPARQL query strings inside `analysis.py`.
- Do not call `st.session_state` directly — use `AnalysisState`.
- Do not hardcode SPARQL endpoint URLs — use the `ENDPOINT_URLS` dict via `post_sparql_with_debug`.
- Do not create new utility functions that duplicate `core/sparql.py`, `core/geometry.py`, `core/naics_utils.py`, `core/boundary.py`.
- Do not skip `render_executed_queries` — it must always appear after the execution block.
- Do not skip `record_executed_query_batch` — ETA learning requires it.
- Do not set `enabled=False` for a new analysis unless the user explicitly asks.
- Do not use `add_grouped_point_layers` for a single-color layer — use `add_point_layer` instead.
