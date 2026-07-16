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

## Workstream A — datasets with configs

**Common sweep completed 2026-07-16** (commit `32f7b5e`): `work_pool` →
`geodata`, `aiddata10` → nova staging paths, `data_manager_version` dropped.
All datasets pinned to image `083d531` (includes cdsapi, scipy, python-dotenv,
and the tmp_to_dst fixes). Remaining per-dataset work is listed below.

| dataset | remaining work | status |
|---|---|---|
| worldpop_pop_count | — | migrated; cluster smoke pending |
| worldpop_pop_count_new | — | new: Global 2015-2030 R2025A; cluster smoke pending |
| critical_habitats | deploy + smoke | |
| cru_ts | deploy + smoke | |
| distance_to_country_border | deploy + smoke | |
| wdpa | deploy + smoke | |
| ookla_speedtest | **anomaly**: has config.toml but zero .py files — investigate | |
| distance_to_coast | `List[]` field; ingest JSON; README; deploy + smoke | |
| distance_to_water | `List[]` field; ingest JSON; README; deploy + smoke | |
| dvnl | `List[]` field; ingest JSON; README; deploy + smoke | |
| gpm | `List[]` field; ingest JSON; README; deploy + smoke | |
| gpw | `List[]` field; ingest JSON; README; deploy + smoke | |
| landscan_pop | `List[]` field; ingest JSON; README; deploy + smoke | |
| malaria_atlas_project | `List[]` field; ingest JSON; README; deploy + smoke | |
| modis_lst | `List[]` field; ingest JSON; README; deploy + smoke | |
| pm25 | `List[]` field; ingest JSON; README; deploy + smoke | |
| plad | `List[]` field; ingest JSON; README; deploy + smoke | |
| udel_climate | `List[]` field; ingest JSON; README; deploy + smoke | |
| worldpop_age_sex | `List[]` field; ingest JSON; README; deploy + smoke | |
| ltdr_ndvi | `List[]` field; ingest JSON; README; **rotate `token`** → `.env`; deploy + smoke | |
| viirs_ntl | `List[]` field; ingest JSON; README; **rotate `password` + `client_secret`** → `.env`; deploy + smoke | |
| oco2 | `List[]` field; ingest JSON; README; **rotate `password`** → `.env`; deploy + smoke (scipy now in image) | |

## Workstream B — legacy, never migrated (~30 dirs)

No config.toml / no data_manager usage; each is a TIGER-style rewrite.
Triage which are still wanted before investing:

`accessibility_map`, `accessibility_to_cities_2015_v1.0`, `acled`,
`africa_child_mortality`, `afrobarometer`, `air_pollution`,
`atlasofurbanexpansion`, `black_marble`*, `boundaries`, `diamond`,
`distance_to_groads`, `dmsp_ols`, `drug`, `gcdf_v3`, `gdp_grid`, `gem`,
`ghs_pop`, `gimms_modis_ndvi`, `global_forest_change`, `globalsolaratlas`,
`globalwindatlas`, `gold`, `historic_gimms_ndvi`, `kummu_gdp_hdi`*,
`landsat7`, `modis_landcover`, `other`, `petroleum`, `speibase`, `srtm`,
`ucdp`

\* `black_marble` and `kummu_gdp_hdi` have partial scripts (no Dataset class).

## Workstream C — ingest JSONs

All 46 legacy `*ingest*.json` files share one old schema; a single conversion
script can restructure them, leaving per-dataset content (tags, citations,
descriptions) for review. Convert each alongside its dataset's migration.

## Batch opportunities

- One commit for the Workstream A common config sweep.
- One dep commit (`scipy`, plus anything found later) → one image rebuild →
  pin all `image_tag`s to that SHA.
- Secrets rotation for `gpw` (`sedac_cookie`), `ltdr_ndvi` (`token`),
  `viirs_ntl` (`username`/`password`/`client_secret`), `oco2` (`password`) —
  all committed to git history, so rotate the credential AND move to the `.env`
  pattern (see checklist item 4). `gpw` was missed by the first audit (the
  secret is named `sedac_cookie`). The auto-generated READMEs for these still
  list the secret as a config var; fix when moving to `.env`.

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
