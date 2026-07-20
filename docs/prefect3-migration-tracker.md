# Prefect 3 migration tracker

Status of migrating datasets to the self-hosted Prefect 3 server
(work pool `geodata`, image `ghcr.io/aiddata/geo-datasets:<sha>`).
Audited 2026-07-16.

## Per-dataset migration checklist

1. **config.toml**: `work_pool = "geodata"`; all paths under
   `/sciclone/nova/REU/geo/...` (`aiddata10` is not mounted in flow pods);
   `image_tag` set to a current CI-built SHA; remove `data_manager_version`
   (vestigial — data_manager is baked into the image).
2. **Flow file**: must contain a `Dataset` subclass (deploy.py requires it;
   the class `.name` becomes the deployment name — config's `deployment_name`
   is ignored) plus a `@flow` wrapper and `get_config` entrypoint.
3. **Dependencies**: every third-party import must be in the root
   `pyproject.toml`/`uv.lock` (that is what the image installs). Missing deps
   require a relock, CI image rebuild, and `image_tag` bump.
4. **Secrets**: never in config.toml. Gitignored `.env` in the dataset dir;
   env var name must equal the config model field name — deploy.py overlays
   `.env` values onto config by key, and the value becomes a deployment
   parameter. Local runs: `__main__` does `load_dotenv()` + explicit assignment.
   Any secret already committed must be **rotated**.
5. **UI-editable list fields**: use comma-separated strings, not `List[...]` —
   the Prefect UI array widget's "add item" button submits the form (frontend
   bug, present through 3.7.8; no schema shape avoids it).
6. **Order**: config-model changes must reach `master` **before** running
   `scripts/deploy.py <dataset>` — the parameter schema is built from master,
   not the working tree.
7. **Ingest JSON**: convert to the esa_landcover `raster_ingest.json` schema
   (all legacy files uniformly miss the same 16 keys — `tags`, `path`,
   `is_global`, `sources_name`, `mapped`, `coverage_dependency`,
   `processing_options`, ... — and carry 5 stale ones: `version`, `options`,
   `base`, `extras`; keep `short_name`). `path` uses the `/data/datasets/<name>`
   convention. `processing_options[].function` must be one of the
   `rasterstats_default_*` functions in the GeoQuery backend
   (`min`/`max`/`mean`/`sum`/`count`/`categorical`).
8. **Interruption-safe writes**: downloads and processing outputs should go
   through `Dataset.tmp_to_dst_file` so an interrupted task cannot leave a
   partial file that a later run mistakes for a complete one. Pass
   `tmp_dir=<dir on the destination filesystem>` — the default `/tmp` is pod
   ephemeral storage, and with many workers × GB-scale files that gets pods
   evicted. (Fixed in data_manager: the helper now preserves umask-standard
   file modes and cleans up its per-file temp directory.)
9. **README**: strip generic uv/deploy boilerplate (lives in central docs);
   keep dataset-specific config documentation and data-source notes. Pattern:
   commit `3778296` (esa_landcover).
10. **Output rasters must be COGs.** `driver="COG", compress="LZW"` (as in
    esa_landcover). For datasets using `distancerasters.rasterize()`: don't
    pass its `output=` kwarg (that writes a plain GTiff via `export_raster`);
    instead take the returned `(array, affine)` and write directly with
    `rasterio` + the COG driver, wrapped in `tmp_to_dst_file(...,
    validate_cog=True)`. See `africa_child_mortality`/`air_pollution`
    `write_cog()` for the pattern.
11. Deploy and smoke-run on the cluster; verify output lands on the share with
    `9198:9915` ownership.

## Done

| dataset | notes |
|---|---|
| geoboundaries | first migration; comma-separated `dl_iso3_list` |
| TIGER | rewritten from argparse script; national layers only |
| esa_landcover | CDS API v2→new endpoint; `CDSAPI_KEY` secret pattern; reference for ingest JSON + README format |
| worldpop_pop_count | migrated; **cluster smoke passed 2026-07-16** |

## Workstream A — datasets with configs

All code-prep waves are **done** across the datasets below: common config sweep
(commit `32f7b5e`), ingest JSONs → current schema (`4ccd6c5`), list fields →
comma strings (`2fe3bfc`, `7682144`), READMEs (`3e88649`, `d42ae20`), and the
`.env` secret pattern (`a0b010e`). All pinned to image `083d531` (cdsapi, scipy,
python-dotenv, tmp_to_dst fixes). What remains is per-dataset **deploy + cluster
smoke**, plus the specific notes below.

| dataset | remaining work | status |
|---|---|---|
| accessibility_map | deploy + smoke | **rebuilt** from a comment stub (JRC access_50k, year 2000); moved from Workstream B |
| africa_child_mortality | deploy + smoke | **rebuilt** from a standalone Py2 rasterize script; moved from Workstream B |
| air_pollution | deploy + smoke | **rebuilt** from a standalone Py2 rasterize script; moved from Workstream B. Source CSV requires a **manual download** (ACS supplementary file is behind a Cloudflare bot challenge, no mirror found) |
| atlasofurbanexpansion | deploy + smoke | **rebuilt** from Py2 `cities_prep.py`/`col_order.py` (200-city sample, 4 boundary levels); moved from Workstream B; live-verified metadata merge + geometry pipeline against real source data |
| distance_to_groads | deploy + smoke | **rebuilt** from `build_dist_to_groads.py` (per-continent shapefiles, dead source); now pulls the single global gROADS v1 file geodatabase via Earthdata bearer token, rasterized directly from the zip (`/vsizip/`, no extraction); moved from Workstream B; COG output; live-verified download + rasterize against real source data |
| gold | deploy + smoke | **rebuilt** from Py2 script; categorical + distance; manual download (ResearchGate); moved from Workstream B |
| gem | deploy + smoke | **rebuilt** from Py2 script; binary + distance; manual download (paivilujala.com); moved from Workstream B |
| drug | deploy + smoke | **rebuilt** from Py2 script; categorical + distance, live-verified multi-layer mix logic; manual download (paivilujala.com); moved from Workstream B |
| diamond | deploy + smoke | **rebuilt** from Py2 script; binary + distance; manual download (PRIO, Entra-gated); moved from Workstream B |
| petroleum | deploy + smoke | **rebuilt** from Py2 script; binary + distance, onshore only; manual download (PRIO, Entra-gated); moved from Workstream B |
| gebco2026 | deploy + smoke | **new**: replaces the planned `srtm` migration (abandoned after discovering SRTMGL3 ships as 14,297 individual tiles — a mosaic wasn't tractable). GEBCO_2026 ships as just 8 global quadrant tiles instead; downloads the ~4.2GB zip (no auth), mosaics the 8 tiles into one seamless global elevation COG via `rasterio.merge` (reads each tile's own georeferencing rather than us parsing filenames; chunked internally so it doesn't need the full ~7GB array in memory), and derives a global slope COG (Horn's method, hand-implemented in numpy — `richdem` was considered but its latest PyPI release has no wheel past Python 3.7 and fails to build from source on 3.12; it also wouldn't solve the degree-of-longitude latitude-scaling problem any better, since that's a per-row correction we apply ourselves regardless of library). Full pipeline verified against the real 4.2GB source (tile placement, seam continuity, slope values) before committing; slope is strip-processed to bound memory (~1.4GB/strip, not the ~30-60GB a naive full-array approach would need) |
| worldpop_pop_count_new | deploy + smoke | new: Global 2015-2030 R2025A |
| critical_habitats | deploy + smoke | |
| cru_ts | deploy + smoke | |
| distance_to_country_border | deploy + smoke | |
| wdpa | deploy + smoke | |
| distance_to_coast | deploy + smoke | |
| distance_to_water | deploy + smoke | |
| gpm | deploy + smoke | |
| landscan_pop | deploy + smoke | |
| malaria_atlas_project | deploy + smoke | merged with travel-time family, see below |
| modis_lst | deploy + smoke | |
| pm25 | deploy + smoke | Box app setup for download |
| plad | deploy + smoke | |
| udel_climate | deploy + smoke | |
| worldpop_age_sex | deploy + smoke | per sex×age×year — large |
| dmsp_ols | deploy + smoke | **rebuilt** from EOG; cookie auth + our Elvidge-2014 calibration; live-verified |
| dvnl | deploy + smoke | EOG cookie auth done + live-verified |
| viirs_ntl | deploy + smoke | EOG cookie auth done + live-verified |
| gpw | deploy + smoke | `.env` cookie/secret; switched to Earthdata dl |
| ltdr_ndvi | deploy + smoke | Earthdata token via `.env`; naming bug fixed |
| oco2 | deploy + smoke | Earthdata token; bumped to 11.2r/11.3r version-by-year; live-verified |

### Secret handling — settled design

`.env` (gitignored, per dataset) → `deploy.py` overlays each line onto the
config → the value becomes a **deployment parameter**. The `.env` key MUST equal
the config model field name (deploy overlays by key match) — the recurring bug
(esa `CDSAPI_KEY`, ltdr `token`/`EARTHDATA_TOKEN`). The token/cookie is stored in
the deployment params (server DB, visible in the Prefect UI); **accepted** since
Prefect access is controlled. No Secret blocks. No outstanding credential
rotations (the previously-flagged ones were placeholders or already expired; a
working token now lives in each `.env`). **Token freshness:** Earthdata tokens
expire (~60 days) and are not auto-refreshed — re-run `deploy.py` with a fresh
`.env` token to rotate oco2/ltdr.

### EOG (eogdata.mines.edu) cookie auth — dvnl, viirs_ntl, dmsp_ols

EOG moved programmatic OAuth access behind a paid tier. These datasets
authenticate with a browser `mod_auth_openidc_session` cookie: config field →
`.env` / deploy param, all EOG GETs send the cookie with `allow_redirects=False`
+ a redirect-to-login guard, and a 30s keep-alive daemon thread holds the
short-lived session open during the download phase. A 30s ping was confirmed to
keep a session alive for a full hour. **Operational:** grab a fresh cookie
immediately before a run (it idles out in minutes); a stale cookie fails loudly
with "redirected to login" rather than writing a login page over a raster.

### Earthdata bearer token — oco2, ltdr_ndvi, distance_to_groads

All three pull from NASA Earthdata Login services (oco2 = GES DISC, ltdr =
LAADS, distance_to_groads = SEDAC via data.earthdata.nasa.gov); a single token
from urs.earthdata.nasa.gov authenticates listing and downloads across all of
them via `Authorization: Bearer <token>`. Field/`.env` key/`__main__` lookup
all named `earthdata_token`. Confirmed live: an anonymous request to the
SEDAC download URL redirects through `urs.earthdata.nasa.gov/oauth/authorize`
to a 401 Basic-auth challenge; the same bearer token already used for
ltdr_ndvi/oco2 works here too (303 → signed CloudFront/S3 URL → real zip
bytes).

## Workstream B — legacy, never migrated (~30 dirs)

No config.toml / no data_manager usage; each is a TIGER-style rewrite.
Triage which are still wanted before investing:

`acled`, `afrobarometer`, `modis_landcover`, `ucdp`,
`gcdf_v3`

### gold, gem, drug, diamond, petroleum — migrated

All five shared one shape: a Py2 script (`fiona.open()` on a hardcoded
`/sciclone/aiddata10/...` shapefile path, `print` statements) that rasterizes
point/polygon deposit data to a global 0.01°, -180/180/-90/90 grid and called
the now-nonexistent `distancerasters.build_distance_array` (replaced by the
`DistanceRaster` class — same fix already applied in `distance_to_groads`).
Ingest JSONs were already on the current schema (Workstream C);
`gold`/`drug`'s `categorical_raster_ingest.json` `mappings` (`none`/type
labels/`mix`) matched each script's category-encoding scheme exactly (loop
index per layer, `mixed_val=4` where a cell has >1 layer) — no
reinterpretation needed, just reimplementation (confirmed by rasterizing real
source data for each and checking the combined output against expectation
before committing).

All five stay **manual-download** (confirmed decision) — a `raw_dir`-placed
source file/shapefile, documented in each README, same pattern as
`air_pollution` — even though `gem`/`drug` resolve via a plain HTTP GET
(kept manual anyway for consistency across the group). `petroleum` stays
**onshore-only**, matching the original script/ingest JSON exactly.

| dataset | shape | source (verified) | placement |
|---|---|---|---|
| gold | categorical (dGOLD_L/S/NL → 1/2/3, mix→4) + distance | ResearchGate — 403s on automated requests | extracted shapefiles per layer: `<raw_dir>/<layer>/<layer>.shp` (zip contents unverifiable — bot-blocked) |
| gem | binary + distance | paivilujala.com/gemdata.html — direct `.zip`, live-verified (1022 features) | zip as-is at `<raw_dir>/gemdata.zip`, read via `/vsizip/` |
| drug | categorical (CANNABIS/COCA BUSH/OPIUM POPPY → 1/2/3, mix→4) + distance | paivilujala.com/drugdata.html — direct `.zip`, live-verified (combine/mix logic confirmed against real overlapping South America data) | zip as-is at `<raw_dir>/drugdata.zip`, read via `/vsizip/` |
| diamond | binary + distance | prio.org/data/10 → `cdn.cloud.prio.org`, confirmed Microsoft Entra (organizational) login gate | extracted `DIADATA.shp` at `<raw_dir>/` (zip contents unverifiable — login-gated) |
| petroleum | binary + distance, onshore only | prio.org/data/11 → same Entra gate | extracted `Petrodata_Onshore_V1.2.shp` at `<raw_dir>/` (zip contents unverifiable) |

For gold/diamond/petroleum the source zip could not be fetched (bot-blocked
or login-gated), so — unlike gem/drug, where the internal zip layout was
confirmed live and the flow reads straight from the zip via `/vsizip/` — these
three expect an already-extracted shapefile directly in `raw_dir` rather than
guessing an unverified internal zip path. All five: `main.py`
(Dataset/flow, COG output via the `write_cog()` pattern), `config.toml`,
README with manual download steps; old `.py`/misc files removed (including
petroleum's dead PBS `job` script and stray lowercase `readme.md`).
Remaining: deploy + cluster smoke.


## Workstream C — ingest JSONs — DONE

All 87 ingest JSONs in the repo are now on the current schema (commits
`4ccd6c5` for the 47 Workstream-A files, `52f939d` for the remaining 36 in
Workstream-B dirs). Rasters use the esa `raster_ingest.json` schema; the 4
atlasofurbanexpansion `*_boundary_ingest.json` files (`type: boundary`) use
GeoQuery's `IngestFeatureCollection` schema instead.

**Review flags (fill when the owning dataset is migrated):**
- Raster ingests: per-dataset `path`, `description`, `is_global` were carried
  over mechanically — worth a human pass.
- atlasofurbanexpansion boundaries: `spatial_extent` left `""` (needs the WKT
  extent, derivable from the `.geojson`) and `is_global` set `false` for a
  200-city sample (confirm).

## Recently completed (beyond the sweep)

- **dmsp_ols rebuilt** (commit `d3c3010`): moved from Workstream B into A. Was
  two dead-repo shell scripts + a Py2 processing.py; now a Dataset/flow pulling
  v4 composites from EOG (cookie auth), applying our Elvidge-2014 calibration →
  COG, plus avg_lights_x_pct download-only. Old scripts in `archive/`.
- **EOG cookie auth** for viirs_ntl (`a231975`), dvnl (`46e15d2`), dmsp_ols.
- **Earthdata token** for oco2 + ltdr_ndvi (`a3383c7`); oco2 also bumped to the
  11.2r/11.3r version-by-year layout (11.1r was removed from GES DISC).
- **Secret naming/rotation thread closed** — see "Secret handling" above.
- **accessibility_map rebuilt**: moved from Workstream B into A. Was a
  one-line comment stub; now downloads the JRC `access_50k.zip` archive and
  rewrites the packaged GeoTIFF as a COG via `/vsizip/` (no on-disk unzip step).
- **accessibility_to_cities_2015_v1.0 merged into malaria_atlas_project**:
  both are Malaria Atlas Project GeoServer products. `malaria_atlas_project`
  is now a multi-dataset flow selecting between **temporal** products
  (archive has one GeoTIFF per year, e.g. `pf_incidence_rate`) and **static**
  ones (single GeoTIFF, e.g. `travel_time_to_cities_2015`) via a `dataset`
  config field and a `DATASET_LOOKUP` table in `main.py`. Also added two more
  static products discovered on the same GeoServer workspace:
  `motorized_travel_time_to_healthcare_2020` and
  `walking_travel_time_to_healthcare_2020` (Weiss et al. 2020, Nature
  Medicine). Each product has its own `<dataset>_raster_ingest.json`. The
  standalone `accessibility_to_cities_2015_v1.0` directory was removed. The
  design is meant to be extended further — adding another MAP product (of
  either shape) needs only a new `DATASET_LOOKUP` entry and ingest JSON, no
  other code changes. See `malaria_atlas_project/README.md`.
- **africa_child_mortality rebuilt**: moved from Workstream B into A. Was a
  standalone Py2 script (`rasterize_childmortality.py`) reading a point file
  from a fixed local path; now a Dataset/flow that downloads the source text
  file directly from its Dropbox share link (`?dl=1`, stable — no need for the
  ephemeral pre-signed `dl.dropboxusercontent.com` URL a browser session
  generates) and rasterizes it per decade with `distancerasters`, writing
  output as a COG (see checklist item 10).
- **air_pollution rebuilt**: moved from Workstream B into A. Was a standalone
  Py2 script (`build_air_pollution_rasters.py`); now a Dataset/flow, COG
  output. The ACS "SI 005" source file is behind a Cloudflare bot challenge
  (confirmed 403 even with a browser User-Agent) with no mirror found, so the
  download step could not be automated — the flow expects
  `GBD2013final.csv` to already be placed in `raw_dir` manually; see
  `air_pollution/README.md`.
- **acled README** refreshed with source/citation/status; still Workstream B
  (no flow — rasterization was done manually in QGIS from point data).
- **atlasofurbanexpansion rebuilt**: moved from Workstream B into A. Was two
  standalone Py2 scripts (`cities_prep.py`, `col_order.py`, the latter tied to
  the old GeoQuery pipeline and dropped). Now a Dataset/flow: downloads the
  Areas & Densities / Blocks & Roads metadata tables and each of the 200
  cities' GIS archives, dissolves each city's boundary per level (`studyArea`,
  `urban_edge_t1/t2/t3`) with `shapely.ops.unary_union`, reprojects to
  EPSG:4326 with `geopandas`, and writes one GeoJSON `FeatureCollection` per
  level. Metadata-merge and geometry logic were verified end-to-end against
  live source data (200 cities, correct dissolve/reproject/MultiPolygon
  output) before committing. The Workstream C review flag on
  `spatial_extent`/`is_global` for the 4 boundary ingests (above) is now
  actionable since the dataset produces the GeoJSONs directly.

## Sweep completed 2026-07-16 (commits 4ccd6c5, 2fe3bfc, 7682144, 3e88649, d42ae20)

- **Ingest JSONs**: all 47 workstream-A files converted to the current schema
  via `scripts/convert_ingest_json.py` (zero drift). Per-dataset `path`,
  `description`, `is_global` still worth a human pass.
- **List → comma-string**: every UI-editable list field converted (years,
  months, methods, download/raster/file lists) across the 15 datasets that had
  them; parse restored in each `__init__`.
- **READMEs**: boilerplate stripped from the 10 that had one; 9 missing ones
  generated from ingest content. All drafts — enrich run-specific detail.

Still per-dataset: deploy + cluster smoke, secrets rotation (above), and
reviewing the generated README/ingest content.
