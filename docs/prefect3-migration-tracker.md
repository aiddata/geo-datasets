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
10. Deploy and smoke-run on the cluster; verify output lands on the share with
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
| worldpop_pop_count_new | deploy + smoke | new: Global 2015-2030 R2025A |
| critical_habitats | deploy + smoke | |
| cru_ts | deploy + smoke | |
| distance_to_country_border | deploy + smoke | |
| wdpa | deploy + smoke | |
| distance_to_coast | deploy + smoke | |
| distance_to_water | deploy + smoke | |
| gpm | deploy + smoke | |
| landscan_pop | deploy + smoke | |
| malaria_atlas_project | deploy + smoke | |
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
| ookla_speedtest | **anomaly**: config.toml but zero .py files — investigate | |

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

### Earthdata bearer token — oco2, ltdr_ndvi

Both pull from NASA Earthdata Login services (oco2 = GES DISC, ltdr = LAADS); a
single token from urs.earthdata.nasa.gov authenticates both listing and
downloads via `Authorization: Bearer <token>`. Field/`.env` key/`__main__`
lookup all named `earthdata_token`.

## Workstream B — legacy, never migrated (~30 dirs)

No config.toml / no data_manager usage; each is a TIGER-style rewrite.
Triage which are still wanted before investing:

`accessibility_map`, `accessibility_to_cities_2015_v1.0`, `acled`,
`africa_child_mortality`, `afrobarometer`, `air_pollution`,
`atlasofurbanexpansion`, `black_marble`*, `boundaries`, `diamond`,
`distance_to_groads`, `drug`, `gcdf_v3`, `gdp_grid`, `gem`,
`ghs_pop`, `gimms_modis_ndvi`, `global_forest_change`, `globalsolaratlas`,
`globalwindatlas`, `gold`, `historic_gimms_ndvi`, `kummu_gdp_hdi`*,
`landsat7`, `modis_landcover`, `other`, `petroleum`, `speibase`, `srtm`,
`ucdp`

\* `black_marble` and `kummu_gdp_hdi` have partial scripts (no Dataset class).

## Workstream C — ingest JSONs

All 46 legacy `*ingest*.json` files share one old schema; a single conversion
script can restructure them, leaving per-dataset content (tags, citations,
descriptions) for review. Convert each alongside its dataset's migration.

## Recently completed (beyond the sweep)

- **dmsp_ols rebuilt** (commit `d3c3010`): moved from Workstream B into A. Was
  two dead-repo shell scripts + a Py2 processing.py; now a Dataset/flow pulling
  v4 composites from EOG (cookie auth), applying our Elvidge-2014 calibration →
  COG, plus avg_lights_x_pct download-only. Old scripts in `archive/`.
- **EOG cookie auth** for viirs_ntl (`a231975`), dvnl (`46e15d2`), dmsp_ols.
- **Earthdata token** for oco2 + ltdr_ndvi (`a3383c7`); oco2 also bumped to the
  11.2r/11.3r version-by-year layout (11.1r was removed from GES DISC).
- **Secret naming/rotation thread closed** — see "Secret handling" above.

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
