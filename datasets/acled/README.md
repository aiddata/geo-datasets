# ACLED Conflict Events

Georeferenced political violence and protest events from the
[Armed Conflict Location & Event Data Project (ACLED)](https://acleddata.com).

**Coverage:** Global, 1997–present  
**Source:** [ACLED data export tool](https://acleddata.com/data-export-tool/)

## Setup

ACLED requires registration. Download the full global dataset (CSV) and place
it in `raw_dir` (default: `/sciclone/nova/REU/geo/geoquery/staging/data/raw/acled`).
The flow picks the most-recently-named CSV if multiple are present.

## Pipeline

1. **Load CSV** — reads the manually downloaded ACLED export; adds a synthetic
   `event_count = 1` column
2. **Convert to GeoPackage** — builds point geometries from `latitude`/`longitude`,
   subsets to filter + outcome columns, writes `acled.gpkg` to `output_dir`
3. **Update filter ingest** — refreshes `acled_filter_ingest.json` with actual
   year ranges, event-type categories, and fatality ranges from the data

Output file: `acled.gpkg` (layer: `acled`)

## Run

```bash
python main.py
```

## Citation

Raleigh, Clionadh, Andrew Linke, Håvard Hegre and Joakim Karlsen. 2010.
Introducing ACLED-Armed Conflict Location and Event Data.
*Journal of Peace Research* 47(5) 651-660.
