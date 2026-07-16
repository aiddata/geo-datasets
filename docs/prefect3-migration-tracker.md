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
   `base`, `short_name`, `extras`).
8. **README**: strip generic uv/deploy boilerplate (lives in central docs);
   keep dataset-specific config documentation and data-source notes. Pattern:
   commit `3778296` (esa_landcover).
9. Deploy and smoke-run on the cluster; verify output lands on the share with
   `9198:9915` ownership.

## Done

| dataset | notes |
|---|---|
| geoboundaries | first migration; comma-separated `dl_iso3_list` |
| TIGER | rewritten from argparse script; national layers only |
| esa_landcover | CDS API v2→new endpoint; `CDSAPI_KEY` secret pattern; reference for ingest JSON + README format |

## Workstream A — datasets with configs (14 remaining)

Common sweep for all: `work_pool` → `geodata`, `aiddata10` → `nova` paths,
`image_tag` bump, drop `data_manager_version`.

| dataset | beyond the common sweep | status |
|---|---|---|
| worldpop_pop_count | `List[]` field; ingest JSON; README | in progress |
| critical_habitats | — | |
| cru_ts | — | |
| distance_to_country_border | — | |
| wdpa | — | |
| ookla_speedtest | **anomaly**: has config.toml but zero .py files — investigate | |
| distance_to_coast | `List[]` field | |
| distance_to_water | `List[]` field | |
| dvnl | `List[]` field | |
| gpm | `List[]` field | |
| gpw | `List[]` field | |
| landscan_pop | `List[]` field | |
| malaria_atlas_project | `List[]` field | |
| modis_lst | `List[]` field | |
| pm25 | `List[]` field | |
| plad | `List[]` field | |
| udel_climate | `List[]` field | |
| worldpop_age_sex | `List[]` field | |
| ltdr_ndvi | `List[]` field; **plaintext `token` in git — rotate** → `.env` | |
| viirs_ntl | `List[]` field; **plaintext `password` + `client_secret` in git — rotate** → `.env` | |
| oco2 | `List[]` field; **plaintext `password` in git — rotate** → `.env`; `scipy` missing from image | |

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
- Secrets rotation for `ltdr_ndvi`, `viirs_ntl`, `oco2` as each is migrated.
