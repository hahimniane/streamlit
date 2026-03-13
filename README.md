# SAWGraph PFAS Explorer

A Streamlit web application for exploring PFAS contamination data using the SAWGraph knowledge graph.

## Features

- **PFAS Upstream Tracing**: Trace contamination upstream to identify potential sources
- **PFAS Downstream Tracing**: Trace contamination downstream from industrial facilities
- **Samples Near Facilities**: Find contaminated samples near specific industry types
- **SOCKG Sites**: Explore SOCKG agricultural sites and nearby facilities

## Quick Start

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py
```



## Project Structure

```
streamlit/
├── app.py                      # Main entry point
├── analysis_registry.py        # Analysis configuration
│
├── analyses/                   # Analysis modules
│   ├── pfas_upstream/          # Upstream tracing
│   ├── pfas_downstream/        # Downstream tracing
│   ├── samples_near_facilities/ # Nearby samples
│   └── sockg_sites/            # SOCKG sites
│
├── core/                       # Core utilities
│   ├── sparql.py               # SPARQL endpoints & execution
│   └── data_loader.py          # Static data loading
│
├── filters/                    # Reusable filter components
│   ├── region.py               # State/county/subdivision selector
│   ├── concentration.py        # Concentration range filter
│   ├── industry.py             # NAICS industry selector
│   ├── substance.py            # PFAS substance filter
│   └── material.py             # Material type filter
│
├── components/                 # UI components
│   └── start_page.py           # Landing page
│
├── data/                       # Static data files
│   ├── us_administrative_regions_fips.csv
│   ├── pfas_substances.csv
│   └── sample_material_types.csv
│
└── assets/                     # Images
    └── Sawgraph-Logo-transparent.png
```

For detailed architecture documentation, see [STRUCTURE.md](STRUCTURE.md).

## Available Analyses

| Analysis | Description | Status |
|----------|-------------|--------|
| **PFAS Upstream Tracing** | Find contaminated samples and trace upstream to identify potential facility sources | Enabled |
| **PFAS Downstream Tracing** | Select an industry type and trace downstream to find contaminated samples | Enabled |
| **Samples Near Facilities** | Find contaminated samples near specific industry types | Enabled |
| **SOCKG Sites** | Explore SOCKG agricultural sites and nearby industrial facilities | Enabled |
| Regional Overview | High-level regional contamination summary | Disabled |
| Facility Risk Assessment | Risk assessment based on proximity and indicators | Disabled |

## Architecture

- **Modular analyses**: Each analysis is self-contained in `analyses/{name}/`
- **Shared filters**: Reusable UI components in `filters/`
- **Centralized SPARQL**: All endpoints and query execution in `core/sparql.py`
- **Configurable regions**: Each analysis declares which region selectors to show

### Key Components

**AnalysisContext** - Shared data passed to each analysis:
```python
@dataclass
class AnalysisContext:
    # Shared data (loaded once, cached)
    states_df: pd.DataFrame
    counties_df: pd.DataFrame
    subdivisions_df: pd.DataFrame
    substances_df: pd.DataFrame
    material_types_df: pd.DataFrame

    # Region selection
    selected_state_code: Optional[str]
    selected_county_code: Optional[str]
    selected_subdivision_code: Optional[str]
    region_code: str      # e.g., "23" or "23005"
    region_display: str   # e.g., "Maine" or "Penobscot County, Maine"

    # Configuration
    endpoints: dict       # SPARQL endpoint URLs
    analysis_key: str     # "upstream", "downstream", etc.
```

**RegionConfig** - Configure region selector per analysis:
```python
@dataclass
class RegionConfig:
    state: Literal["required", "optional", "hidden"] = "optional"
    county: Literal["required", "optional", "hidden"] = "optional"
    subdivision: Literal["required", "optional", "hidden"] = "optional"
    availability_source: Literal["pfas", "sockg", None] = "pfas"
```

## SPARQL Endpoints

| Endpoint | Description |
|----------|-------------|
| `sawgraph` | PFAS contamination observations |
| `spatial` | Administrative boundaries and spatial relationships |
| `hydrology` | Water flow networks (NHDPlus V2) |
| `fio` | Industrial facilities (NAICS data) |
| `federation` | Federated endpoint for cross-graph queries |

## Adding a New Analysis

1. Create folder: `analyses/my_analysis/`
2. Add `__init__.py` (empty file)
3. Add `queries.py` with SPARQL query functions
4. Add `analysis.py` with `main(context: AnalysisContext)` function
5. Register in `analysis_registry.py`

See [STRUCTURE.md](STRUCTURE.md) for detailed instructions.

## Dependencies

Key dependencies (see `requirements.txt` for full list):
- `streamlit` - Web framework
- `pandas` - Data manipulation
- `geopandas` - Geospatial data
- `folium` - Interactive maps
- `SPARQLWrapper` - SPARQL queries
- `st-ant-tree` - Hierarchical dropdown selector

## License


