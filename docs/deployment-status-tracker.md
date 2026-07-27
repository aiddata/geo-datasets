# Deployment status tracker

Columns:
- **Existing** — does data already exist at the expected location on disk?
- **Symlink** — does the prod/staging symlink for this dataset exist?
- **Ran** — has a Prefect deployment been run for this flow?
- **Verified** — has the output data been manually checked/QA'd?
- **Prod** — is this dataset live in the production GeoQuery environment?
- **Ingested** — has GeoQuery actually ingested this dataset?

| Dataset                                    | Existing | Symlink | Ran | Verified | Prod | Ingested |
|--------------------------------------------|----------|---------|-----|----------|------|----------|
| `accessibility_map`                        | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `acled`                                    | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `africa_child_mortality`                   | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `air_pollution - o3`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `air_pollution - pm25`                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `critical_habitats`                        | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `cru_ts - monthly_pre`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `cru_ts - monthly_tmp`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `cru_ts - yearly_pre`                      | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `cru_ts - yearly_tmp`                      | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `diamond - binary`                         | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `diamond - distance`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_coast`                        | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_country_border`               | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_groads`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_water`                        | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `dmsp_ols`                                 | [?]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `drug - categorical`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `drug - distance`                          | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `dvnl`                                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `esa_landcover`                            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gcdf_v3_dynamic`                          | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gebco2026 - elevation`                    | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gebco2026 - slope`                        | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gem - binary`                             | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gem - distance`                           | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalsolaratlas - pvout`                 | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalwindatlas - pd`                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalwindatlas - ws`                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gold - categorical`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gold - distance`                          | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpm - monthly`                            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpm - yearly`                             | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v3_count`                           | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v3_density`                         | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4_count`                           | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4_density`                         | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4rev11_count`                      | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4rev11_density`                    | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `landscan_pop`                             | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `ltdr_ndvi - v6_monthly`                   | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `ltdr_ndvi - v6_yearly`                    | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `map - motorized_healthcare_2020`          | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `map - pf_incidence_rate`                  | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `map - cities_2015`                        | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `map - walking_healthcare_2020`            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `modis_landcover`                          | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/monthly_day`              | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/monthly_night`            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/yearly_day`               | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/yearly_night`             | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `oco2 - monthly_xco2`                      | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `oco2 - yearly_xco2`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `petroleum - binary`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `petroleum - distance`                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `plad`                                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `pm25 - monthly`                           | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `pm25 - yearly`                            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `srtm - srtm_elevation_500m`               | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `srtm - srtm_slope_500m`                   | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `ucdp - ged261_filter_ingest.json`         | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `udel_climate - air_temp_2017_yearly_mean` | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `udel_climate - precip_2017_yearly_mean`   | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `udel_climate - precip_2017_yearly_sum`    | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - annual_v20_avg_masked`            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - annual_v20_cf_cvg`                | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - annual_v21_avg_masked`            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - annual_v21_cf_cvg`                | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - annual_v22_avg_masked`            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - annual_v22_cf_cvg`                | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - monthly_avg_masked`               | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - monthly_cf_cvg`                   | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `wdpa - wdpa_iucn`                         | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `worldpop_pop_count`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `worldpop_pop_count_new`                   | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
