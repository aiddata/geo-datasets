# geoBoundaries

Administrative boundary data from the
[geoBoundaries](https://www.geoboundaries.org) project (William & Mary / AidData).

**Coverage:** Global, all admin levels  
**Source:** [geoBoundaries Open](https://github.com/wmgeolab/geoBoundaries)

## Config

| field | description |
|---|---|
| `gb_version` | geoBoundaries release ID (e.g. `gb80` = v8.0); pins the run to that release's immutable snapshot |
| `dl_iso3_list` | comma-separated ISO3 codes to download; empty = all countries |
| `output_dir` | destination directory; outputs land under `<output_dir>/<gb_version>/` |
| `skip_existing` | skip countries whose GPKG + metadata JSON already exist |

## Pipeline

1. **Resolve release** — looks up `gb_version` in `https://www.geoboundaries.org/api/releases` to get the
   release's version number (e.g. `8.0`) and immutable snapshot hash; fails with the list of available
   releases if the ID is unknown. Both appear in the output metadata (titles and `details`)
2. **Fetch index** — reads `https://www.geoboundaries.org/api/<gb_version>/gbOpen/ALL/ALL/`, a flat JSON list
   of boundaries for that release; download URLs point into the release snapshot
3. **Filter** — subsets to `dl_iso3_list` if non-empty; otherwise downloads all
4. **Download + convert** — for each ISO3/admin-level entry: fetches the GeoJSON
   from geoboundaries.org, converts to GeoPackage, writes a raw metadata JSON alongside it

Output per item: `<gb_version>/<stem>.gpkg` + `<gb_version>/raw_<stem>.json` (+ `<stem>.json` GeoQuery ingest metadata)

## Run

```bash
python main.py
```
