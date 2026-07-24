# Ingest path reconciliation tracker

Audited 2026-07-21, revised after a second pass. Scope: every dataset
directory with both a `config.toml` and ≥1 ingest JSON (33 directories).

**The actual issue:** `config.toml`'s `output_dir` plus whatever
subdirectories `main.py` appends per output file are correct — that's where
files really land. Ingest JSON `path` fields, in a lot of cases, don't
mirror that real directory structure: they use a flattened, independently
invented name instead of the real hierarchical path. `file_mask` correctness
is a separate, secondary concern (not the focus here) — the fix in every
row below is **update the ingest JSON's `path`**, not the code.

Paths below are given relative to `/data/datasets/` (the ingest side's
common prefix) — i.e. the value to put after `/data/datasets/` in each
`path` field.

## Already fixed (this session)

| dataset | real path | ingest path |
|---|---|---|
| `gebco2026` | `gebco/gebco2026/elevation`, `gebco/gebco2026/slope` | matches |
| `gold` | `gold/categorical`, `gold/distance` | matches |
| `drug` | `drug/categorical`, `drug/distance` | matches |

## Not an issue

`atlasofurbanexpansion` — boundary-type ingests (`type: boundary`) can
validly have multiple files share one directory, unlike raster's
`file_mask: "None"` convention. User corrected `output_dir` independently
(now under `.../data/boundaries/atlasofurbanexpansion`). No further action.

## Fixed — simple (flat real path, ingest path just used a different name)

| dataset | ingest file | real path | current ingest `path` | → set `path` to |
|---|---|---|---|---|
| `accessibility_map` | raster_ingest.json | `access_50k` | `access_50k` | *(already correct)* |
| `africa_child_mortality` | raster_ingest.json | `africa_child_mortality` | `africa_child_mortality` | *(already correct)* |
| `dmsp_ols` | raster_ingest.json | `dmsp_ols` | `dmsp_ols` | *(already correct)* |
| `ucdp` | ged261_filter_ingest.json | `ucdp/ged261.gpkg` | `ucdp/ged261.gpkg` | *(already correct)* |
| `acled` | acled_filter_ingest.json | `acled/acled.gpkg` | `acled/acled.gpkg` | *(already correct)* |
| `esa_landcover` | raster_ingest.json | `esa_landcover` | `esa_lc` | `esa_landcover` |
| `dvnl` | raster_ingest.json | `dvnl` | `dvnl_2022` | `dvnl` |
| `distance_to_country_border` | raster_ingest.json | `distance_to_country_borders` | `dist_to_geoBoundaries_borders` | `distance_to_country_borders` |
| `distance_to_groads` | raster_ingest.json | `distance_to_groads` | `dist_to_groads` | `distance_to_groads` |
| `modis_landcover` | raster_ingest.json | `MODIS/MCD12Q1.061` | `modis_landcover_61` | `MODIS/MCD12Q1.061` |
| `landscan_pop` | raster_ingest.json | `landscan/population` | `landscan_global_population` | `landscan/population` |
| `worldpop_pop_count` | 1km_mosaic_raster_ingest.json | `worldpop/population_counts/1km_mosaic` | `worldpop_pop_count` | `worldpop/population_counts/1km_mosaic` |
| `worldpop_pop_count_new` | 1km_mosaic_raster_ingest.json | `worldpop/population_counts/1km_mosaic_r2025a` | `worldpop_pop_count_r2025a` | `worldpop/population_counts/1km_mosaic_r2025a` |
| `gcdf_v3_dynamic` | filter_ingest.json | `gcdf_v3_dynamic/gcdf_v301_dynamic.gpkg` (real `output_dir` basename is `gcdf_v3_dynamic`, not `gcdf_v301_dynamic`) | `gcdf_v301_dynamic/gcdf_v301_dynamic.gpkg` | `gcdf_v3_dynamic/gcdf_v301_dynamic.gpkg` |

(First 5 rows confirmed already correct — included for completeness since
they were re-checked this pass, not because they need changes.)

## Fixed — versioned (config-driven version string is part of the real path)

| dataset | ingest file | real path | current ingest `path` | → set `path` to |
|---|---|---|---|---|
| `critical_habitats` | raster_ingest.json | `critical_habitats/2` (version=2 in config) | `critical_habitats_v2` | `critical_habitats/2` |
| `distance_to_coast` | raster_ingest.json | `distance_to_coast/2.3.7/distance` (gshhg_version) | `dist_to_coast_237` | `distance_to_coast/2.3.7/distance` |
| `distance_to_water` | raster_ingest.json | `distance_to_water/2.3.7_d4533ef/distance` (gshhg_version_ne_hash[:7]) | `dist_to_water_237_d4533ef` | `distance_to_water/2.3.7_d4533ef/distance` |
| `wdpa` | wdpa_iucn_raster_ingest.json | `wdpa/<version>/iucn_cat` — version comes from the downloaded file's name at run time (currently `202406`) | `wdpa_iucn_cat_202406` | `wdpa/202406/iucn_cat` (**re-check this one at deploy time** — version isn't a fixed config value, it's derived from whatever WDPA release was last downloaded) |
| `plad` | raster_ingest.json | `plad/v70` | `plad_v70_leader_birthplace` | `plad/v70` |
| `pm25` | yearly_raster_ingest.json | `pm25/V5GL04/Global/Annual` | `surface_pm25_annual_V5GL04` | `pm25/V5GL04/Global/Annual` |
| `pm25` | monthly_raster_ingest.json | `pm25/V5GL04/Global/Monthly` | `surface_pm25_monthly_V5GL04` | `pm25/V5GL04/Global/Monthly` |

## Fixed — multi-output (binary + distance)

The previously-flagged asymmetry (binary in its own subdir, distance flat in
the parent) is also fixed now — `main.py` for all three gives distance its
own subdir too, matching binary's layout.

| dataset | ingest file | path |
|---|---|---|
| `diamond` | binary_raster_ingest.json | `diamond/binary` |
| `diamond` | distance_raster_ingest.json | `diamond/distance` |
| `gem` | binary_raster_ingest.json | `gem/binary` |
| `gem` | distance_raster_ingest.json | `gem/distance` |
| `petroleum` | binary_raster_ingest.json | `petroleum/binary` |
| `petroleum` | distance_raster_ingest.json | `petroleum/distance` |

## Fixed — multi-output (period/variable subdirs)

| dataset | ingest file | real path | → set `path` to |
|---|---|---|---|
| `air_pollution` | o3_raster_ingest.json | `air_pollution/o3` | `air_pollution/o3` |
| `air_pollution` | pm25_raster_ingest.json (this is the fus_calibrated/PM2.5 ingest, confusingly named) | `air_pollution/fus_calibrated` | `air_pollution/fus_calibrated` |
| `gpm` | monthly_raster_ingest.json | `gpm/v07b/monthly` | `gpm/v07b/monthly` |
| `gpm` | yearly_raster_ingest.json | `gpm/v07b/yearly/mean` (year_agg_method=mean) | `gpm/v07b/yearly/mean` |
| `cru_ts` | monthly_tmp_raster_ingest.json | `cru_ts/cru_ts_4.07/monthly/tmp` | `cru_ts/cru_ts_4.07/monthly/tmp` |
| `cru_ts` | yearly_tmp_raster_ingest.json | `cru_ts/cru_ts_4.07/yearly/tmp/mean` | `cru_ts/cru_ts_4.07/yearly/tmp/mean` |
| `cru_ts` | monthly_pre_raster_ingest.json | `cru_ts/cru_ts_4.07/monthly/pre` | `cru_ts/cru_ts_4.07/monthly/pre` |
| `cru_ts` | yearly_pre_raster_ingest.json | `cru_ts/cru_ts_4.07/yearly/pre/mean` | `cru_ts/cru_ts_4.07/yearly/pre/mean` |
| `oco2` | monthly_xco2_raster_ingest.json | `gesdisc/OCO2_L2_Lite_FP/xco2/month_interp` | `gesdisc/OCO2_L2_Lite_FP/xco2/month_interp` |
| `oco2` | yearly_xco2_raster_ingest.json | `gesdisc/OCO2_L2_Lite_FP/xco2/year_interp` | `gesdisc/OCO2_L2_Lite_FP/xco2/year_interp` |
| `gpw` | gpw_v4rev11_count_raster_ingest.json | `gpw/gpw_v4_rev11/count` | `gpw/gpw_v4_rev11/count` |
| `gpw` | gpw_v4rev11_density_raster_ingest.json | `gpw/gpw_v4_rev11/density` | `gpw/gpw_v4_rev11/density` |
| `ltdr_ndvi` | v5_yearly_raster_ingest.json | `ltdr_ndvi/yearly` | `ltdr_ndvi/yearly` |
| `ltdr_ndvi` | v5_monthly_raster_ingest.json | `ltdr_ndvi/monthly` | `ltdr_ndvi/monthly` |
| `modis_lst` (`ingest/061/`) | monthly_day_ingest.json | `MODIS/terra/MOLT/MOD11C3.061/monthly/day` | same |
| `modis_lst` (`ingest/061/`) | monthly_night_ingest.json | `MODIS/terra/MOLT/MOD11C3.061/monthly/night` | same |
| `modis_lst` (`ingest/061/`) | yearly_day_ingest.json | `MODIS/terra/MOLT/MOD11C3.061/yearly/day/mean` | same |
| `modis_lst` (`ingest/061/`) | yearly_night_ingest.json | `MODIS/terra/MOLT/MOD11C3.061/yearly/night/mean` | same |
| `malaria_atlas_project` | pf_incidence_rate_raster_ingest.json | `malaria_atlas_project/pf_incidence_rate` | same |
| `malaria_atlas_project` | travel_time_to_cities_2015_raster_ingest.json | `malaria_atlas_project/travel_time_to_cities_2015` | same |
| `malaria_atlas_project` | motorized_travel_time_to_healthcare_2020_raster_ingest.json | `malaria_atlas_project/motorized_travel_time_to_healthcare_2020` | same |
| `malaria_atlas_project` | walking_travel_time_to_healthcare_2020_raster_ingest.json | `malaria_atlas_project/walking_travel_time_to_healthcare_2020` | same |
| `udel_climate` | udel_precip_2017_yearly_mean_raster_ingest.json | `udel_climate/precip_2017/yearly/mean` | same |
| `udel_climate` | udel_precip_2017_yearly_sum_raster_ingest.json | `udel_climate/precip_2017/yearly/sum` | same |
| `udel_climate` | udel_air_temp_2017_yearly_mean_raster_ingest.json | `udel_climate/air_temp_2017/yearly/mean` | same |

Note: `malaria_atlas_project`'s current `path`s use a `map_` prefix instead
of the real `malaria_atlas_project/` directory name (e.g.
`map_pf_incidence_rate` → `malaria_atlas_project/pf_incidence_rate`) — same
pattern as everything else in this table, just called out since that
dataset's mismatch is a flat rename rather than a dropped hierarchy level.

## Fixed — `viirs_ntl` (was: needs a decision)

`main.py` now writes to `output_dir/{annual,monthly}/{version}/{avg_masked,cf_cvg}/`
instead of flat filenames with no version component — v20/v21/v22 annual
and v10 monthly (hardcoded, no config field) each get their own directory,
so switching `annual_version` no longer overwrites a previous version's
output. `write_cog`'s `tmp_to_dst_file` call picked up `make_dst_dir=True`
since these subdirs no longer get created by `main()`'s single
`os.makedirs(output_dir)` call. All 8 ingest JSON `path`s updated to match:

| ingest file | path |
|---|---|
| annual_v20_avg_masked | `viirs_ntl/annual/v20/avg_masked` |
| annual_v20_cf_cvg | `viirs_ntl/annual/v20/cf_cvg` |
| annual_v21_avg_masked | `viirs_ntl/annual/v21/avg_masked` |
| annual_v21_cf_cvg | `viirs_ntl/annual/v21/cf_cvg` |
| annual_v22_avg_masked | `viirs_ntl/annual/v22/avg_masked` |
| annual_v22_cf_cvg | `viirs_ntl/annual/v22/cf_cvg` |
| monthly_avg_masked | `viirs_ntl/monthly/v10/avg_masked` |
| monthly_cf_cvg | `viirs_ntl/monthly/v10/cf_cvg` |

- ~~Orphaned ingest JSONs~~ — **acknowledged, no action.** Ignoring these
  per direction:
  - `gpw`: `gpw_v3_count_raster_ingest.json`, `gpw_v3_density_raster_ingest.json`, `gpw_v4_count_raster_ingest.json`, `gpw_v4_density_raster_ingest.json`
  - `ltdr_ndvi`: `v4_yearly_raster_ingest.json`
  - `modis_lst`: all 4 files under `ingest/006/` (only `.061` is configured)
  - `udel_climate`: 6 files under `ingest/udel_air_temp_2014/` and `ingest/udel_precip_2014/` (also on a legacy `"base"` schema, not just orphaned)

## Out of scope — **acknowledged, no action** (static ones with no flow)

- `srtm`, `globalsolaratlas`, `globalwindatlas` — ingest JSONs present,
  manual-download READMEs only, no flow.
- `gcdf_v3/gcdf_v3_boundaries`, `gcdf_v3/gcdf_v3_static` — generated by a
  separate repo (`aiddata/china-osm-geodata`), per `gcdf_v3/README.md`.

## Status

All items resolved. Orphaned ingests and no-flow static datasets
acknowledged as out of scope (no action needed).
