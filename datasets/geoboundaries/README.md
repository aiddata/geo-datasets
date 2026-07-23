# geoBoundaries

Administrative boundary data from the
[geoBoundaries](https://www.geoboundaries.org) project (William & Mary / AidData).

**Coverage:** Global, all admin levels  
**Source:** [geoBoundaries Open](https://github.com/wmgeolab/geoBoundaries)

## Config

| field | description |
|---|---|
| `version` | geoBoundaries release tag (e.g. `v6`) |
| `gb_web_hash` | `gbWeb` repo commit hash — pinned to a specific release snapshot |
| `dl_iso3_list` | comma-separated ISO3 codes to download; empty = all countries |
| `output_dir` | destination directory; outputs land under `<output_dir>/<gb_web_hash>/` |
| `skip_existing` | skip countries whose GPKG + metadata JSON already exist |

## Pipeline

1. **Fetch index** — reads the gbWeb API JSON for the pinned `gb_web_hash`
2. **Filter** — subsets to `dl_iso3_list` if non-empty; otherwise downloads all
3. **Download + convert** — for each ISO3/admin-level entry: fetches the GeoJSON
   from GitHub, converts to GeoPackage, writes a raw metadata JSON alongside it

Output per item: `<gb_web_hash>/<stem>.gpkg` + `<gb_web_hash>/raw_<stem>.json`

## Run

```bash
python main.py
```
