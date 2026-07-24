# TIGER/Line Shapefiles

US Census Bureau national-level administrative boundary layers.

**Coverage:** United States  
**Source:** [Census TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)

One flow run = one layer for one year. Per-state layers (tract, block group)
are out of scope; only national layers (e.g. COUNTY, STATE) are supported.

## Config

| field | description |
|---|---|
| `year` | 4-digit census year (e.g. `2025`) |
| `dataset` | layer name as it appears in the TIGER URL (e.g. `COUNTY`, `STATE`) |
| `raw_dir` | directory for the downloaded zip |
| `output_dir` | directory for the output GeoPackage and generated ingest JSON |

## Pipeline

1. **Download** — fetches `tl_<year>_us_<dataset>.zip` from `www2.census.gov`
2. **Process** — reads the zip into GeoPandas, fills nulls, writes a GeoPackage
3. **Ingest JSON** — generates `ingest_<year>_<dataset>.json` alongside the GPKG

Output: `TIGER/tl_<year>_us_<dataset>.gpkg` + `TIGER/ingest_<year>_<dataset>.json`

## Run

```bash
python main.py
```

## Citation

U.S. Census Bureau. TIGER/Line Shapefiles. U.S. Department of Commerce.
https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
