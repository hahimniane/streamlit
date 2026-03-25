# SAWGraph PFAS Explorer - Project Structure

## Directory Layout

```
streamlit/
├── app.py                      # Main entry point
├── analysis_registry.py        # Analysis configuration and registry
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation
├── STRUCTURE.md                # This file
│
├── analyses/                   # Analysis modules (each self-contained)
│   ├── __init__.py
│   ├── pfas_upstream/          # Upstream tracing analysis
│   │   ├── __init__.py
│   │   ├── analysis.py         # UI and orchestration
│   │   └── queries.py          # SPARQL queries
│   ├── pfas_downstream/        # Downstream tracing analysis
│   │   ├── __init__.py
│   │   ├── analysis.py
│   │   └── queries.py
│   ├── samples_near_facilities/ # Nearby samples analysis
│   │   ├── __init__.py
│   │   ├── analysis.py
│   │   └── queries.py
│   ├── sockg_sites/            # SOCKG sites analysis
│   │   ├── __init__.py
│   │   ├── analysis.py
│   │   └── queries.py
│   ├── regional_overview/      # Regional overview (disabled)
│   │   ├── __init__.py
│   │   └── analysis.py
│   └── facility_risk/          # Facility risk (disabled)
│       ├── __init__.py
│       └── analysis.py
│
├── core/                       # Core utilities (generic, not domain-specific)
│   ├── __init__.py
│   ├── sparql.py               # SPARQL endpoints, parsing, execution
│   └── data_loader.py          # Static data loading (FIPS, substances, etc.)
│
├── filters/                    # Domain-specific filters (UI + data)
│   ├── __init__.py
│   ├── region.py               # Region selection (state/county/subdivision)
│   ├── substance.py            # PFAS substance filtering
│   ├── material.py             # Material type filtering
│   ├── concentration.py        # Concentration range UI + queries
│   └── industry.py             # NAICS industry data + selector
│
├── components/                 # Generic UI components
│   ├── __init__.py
│   └── start_page.py           # Landing page
│
├── data/                       # Static data files
│   ├── pfas_substances.csv
│   ├── sample_material_types.csv
│   └── us_administrative_regions_fips.csv
│
└── assets/
    └── Sawgraph-Logo-transparent.png
```

## Architecture Overview

### 1. Entry Point (`app.py`)
- Configures Streamlit page
- Loads shared data once (cached)
- Builds analysis registry
- Renders sidebar with analysis selector and region filters
- Dispatches to selected analysis

### 2. Analysis Registry (`analysis_registry.py`)
- `AnalysisContext` - Shared data/config passed to analyses
- `AnalysisSpec` - Analysis metadata and configuration
- `RegionConfig` - Region selector configuration per analysis
- `build_registry()` - Returns all available analyses with lazy imports

### 3. Analyses (`analyses/`)
Each analysis is self-contained in its own folder:
- `analysis.py` - UI rendering, user interaction, result display
- `queries.py` - SPARQL queries specific to this analysis

Benefits:
- Add new analyses easily (create folder with analysis.py + queries.py)
- Modify queries without touching UI code
- Clear ownership of code

### 4. Core (`core/`)
Generic utilities that are not domain-specific:

**`sparql.py`** - Single source of truth for SPARQL operations:
- `ENDPOINT_URLS` - All SPARQL endpoint URLs
- `get_sparql_wrapper()` - Create configured SPARQLWrapper
- `parse_sparql_results()` - Parse JSON results to DataFrame
- `execute_sparql_query()` - Execute query via HTTP
- `convertToDataframe()` - Convert SPARQLWrapper2 results

**`data_loader.py`** - Static data loading:
- `load_fips_data()` - Load FIPS codes CSV
- `load_substances_data()` - Load substances CSV
- `load_material_types_data()` - Load material types CSV
- `load_naics_dict()` - Load NAICS 2022 code→title from `data/naics_2022.csv`
- `parse_regions()` - Parse FIPS into states/counties/subdivisions

### 5. Filters (`filters/`)
Domain-specific filters combining UI widgets and data queries:

**`region.py`** - Geographic region selection:
- `RegionConfig` - Configure which region levels are shown
- `RegionSelection` - Container for selected region data
- `render_region_selector()` - Unified region selector UI
- `get_region_boundary()` - Get WKT boundary for mapping

**`substance.py`** - PFAS substance filtering:
- `get_available_substances_with_labels()` - Substances in a region

**`material.py`** - Sample material type filtering:
- `get_available_material_types_with_labels()` - Material types in a region

**`concentration.py`** - Concentration range filter:
- `render_concentration_filter()` - Min/max inputs, slider, nondetects checkbox
- `apply_concentration_filter()` - Apply pending values on execute
- `get_max_concentration()` - Max concentration for region/filters

**`industry.py`** - NAICS industry selection:
- NAICS reference data is loaded from `data/naics_2022.csv` via `core.data_loader.load_naics_dict()`
- `render_hierarchical_naics_selector()` - Tree-based industry selector
- `build_naics_hierarchy()` - Build hierarchy from flat codes

### 6. Components (`components/`)
Generic UI components not tied to a specific analysis:
- `start_page.py` - Landing page with app description

## Map Color System

All map colors are defined in `components/map_rendering.py` and derive from
[ColorBrewer2](https://colorbrewer2.org/) palettes, following EPA/USGS
cartographic standards. The authoritative reference is the
[Design System wiki](https://github.com/SAWGraph/explorer-app/wiki/Design-System-References).

### Palettes at a Glance

| Category | Palette | Constants | Purpose |
|---|---|---|---|
| Water features | PuBu sequential (6-class) | `COLOR_WATERSHED`, `COLOR_AQUIFER`, `COLOR_FLOWLINE`, `COLOR_WELL` | Watersheds, aquifers, flowlines, wells |
| Sample points | PuOr diverging (8-class) | `COLOR_SAMPLE`, `SAMPLE_PUOR_PALETTE`, `SAMPLE_CONC_BREAKS` | PFAS concentration gradient (purple = low, orange = high) |
| Facilities (primary) | Reds sequential (9-class) | `FACILITY_COLORS_REDS` | Industry layers when PFAS signatures are present |
| Facilities (secondary) | Purples sequential (9-class) | `FACILITY_COLORS_PURPLES` | Industry layers when PFAS signatures are absent |

### Water Features — PuBu Sequential

Fixed single-color assignments:

| Constant | Hex | Use |
|---|---|---|
| `COLOR_WATERSHED` | `#d0d1e6` | Watershed boundaries |
| `COLOR_AQUIFER` | `#74a9cf` | Aquifer outlines (striped pattern) |
| `COLOR_FLOWLINE` | `#2b8cbe` | Stream / river flowlines |
| `COLOR_WELL` | `#045a8d` | Well points |

### Sample Points — PuOr Diverging

Each sample point is colored by its `overall_max_result` concentration (ng/L).
The mapping is handled by `_concentration_to_color()` and `sample_point_style()`:

| Index | Hex | Concentration range |
|---|---|---|
| 0 | `#542788` | Non-detect / zero |
| 1 | `#8073ac` | 0 < c ≤ 4 ng/L |
| 2 | `#b2abd2` | 4 < c ≤ 20 |
| 3 | `#d8daeb` | 20 < c ≤ 50 |
| 4 | `#fee0b6` | 50 < c ≤ 100 |
| 5 | `#fdb863` | 100 < c ≤ 200 |
| 6 | `#e08214` | 200 < c ≤ 400 |
| 7 | `#b35806` | > 400 |

The break points are stored in `SAMPLE_CONC_BREAKS = [0, 4, 20, 50, 100, 200, 400]`.

Marker radius also scales with concentration (see `sample_point_style()`).
Use `add_sample_layer()` — never `add_point_layer()` — for sample points so
the PuOr styling is applied automatically.

### Facilities — Reds / Purples Sequential

When facilities are rendered as grouped layers (one layer per industry type via
`add_grouped_point_layers()`), colors cycle through `LAYER_COLORS`.

The full 9-class ColorBrewer palettes are stored as `_FACILITY_COLORS_REDS_FULL`
and `_FACILITY_COLORS_PURPLES_FULL`. The **exported constants**
`FACILITY_COLORS_REDS` and `FACILITY_COLORS_PURPLES` skip the 3 lightest
shades (indices 0–2) because those are nearly white and invisible as both
map markers and layer-control text. The visible subsets are:

**Reds (primary — `FACILITY_COLORS_REDS` / `LAYER_COLORS`):**

| Hex | Approx. description |
|---|---|
| `#fc9272` | Salmon |
| `#fb6a4a` | Coral |
| `#ef3b2c` | Red |
| `#cb181d` | Crimson |
| `#a50f15` | Dark red |
| `#67000d` | Maroon |

**Purples (secondary — `FACILITY_COLORS_PURPLES`):**

| Hex | Approx. description |
|---|---|
| `#bcbddc` | Lavender |
| `#9e9ac8` | Medium purple |
| `#807dba` | Purple |
| `#6a51a3` | Dark purple |
| `#54278f` | Deep purple |
| `#3f007d` | Very dark purple |

Colors wrap around via modulo when there are more groups than colors.

### Marker Sizing

| Constant | Value | Use |
|---|---|---|
| `DEFAULT_POINT_RADIUS` | 6 | Generic point layers |
| `FACILITY_MARKER_RADIUS` | 8 | Grouped facility layers |
| `OTHER_FACILITY_MARKER_RADIUS` | 6 | Secondary facility layers |
| `PFAS_FACILITY_MARKER_RADIUS` | 7 | PFAS-specific facility layers |

### How to Use in an Analysis

```python
# Sample points (auto-colored by concentration)
add_sample_layer(map_obj, samples_gdf, popup_fields=[...])

# Single-color point layer
add_point_layer(map_obj, gdf, name='...', color='DarkOrange', ...)

# Grouped facility layers (auto-cycles through LAYER_COLORS)
add_grouped_point_layers(map_obj, gdf, group_column='industryName', ...)

# Flowlines
add_line_layer(map_obj, gdf, name='...', color=COLOR_FLOWLINE, ...)
```

## Key Design Principles

1. **Separation of Concerns**: Queries are separated from UI logic
2. **Single Source of Truth**: One copy of shared utilities (e.g., `parse_sparql_results`)
3. **Self-Contained Analyses**: Each analysis folder has everything it needs
4. **Configurable Region Selector**: Analyses declare their region requirements
5. **Reusable Filters**: Concentration, industry, etc. are shared across analyses
6. **Lazy Loading**: Analyses are imported only when needed
7. **Cached Data**: Static data and availability queries are cached

## Adding a New Analysis

1. Create folder: `analyses/my_new_analysis/`
2. Create `__init__.py` (empty)
3. Create `queries.py` with SPARQL query functions
4. Create `analysis.py` with `main(context: AnalysisContext)` function
5. Register in `analysis_registry.py` with `AnalysisSpec`

Example registration:
```python
AnalysisSpec(
    key="my_analysis",
    label="My New Analysis",
    title="🔍 My New Analysis",
    description="Description of what this analysis does.",
    query=6,
    enabled=True,
    runner=my_analysis_main,
    region_config=RegionConfig(
        state="optional",
        county="optional",
        subdivision="optional",
        availability_source="pfas",
    ),
),
```

## Data Flow

```
User selects analysis
        ↓
app.py loads shared data (cached)
        ↓
app.py renders region selector based on analysis.region_config
        ↓
User makes region selection
        ↓
app.py creates AnalysisContext with all data
        ↓
app.py calls analysis.main(context)
        ↓
Analysis renders sidebar (filters, parameters)
        ↓
User clicks Execute
        ↓
Analysis calls queries.py functions
        ↓
Analysis displays results and map
```
