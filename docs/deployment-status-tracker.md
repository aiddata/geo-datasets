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
| `accessibility_map`                        | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `acled`                                    | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `africa_child_mortality`                   | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `air_pollution - o3`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `air_pollution - pm25`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `critical_habitats`                        | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `cru_ts - monthly_pre`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `cru_ts - monthly_tmp`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `cru_ts - yearly_pre`                      | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `cru_ts - yearly_tmp`                      | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `diamond - binary`                         | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `diamond - distance`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_coast`                        | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_country_border`               | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_groads`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_water`                        | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `dmsp_ols`                                 | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `drug - categorical`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `drug - distance`                          | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `dvnl`                                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `esa_landcover`                            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gcdf_v3_dynamic`                          | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gebco2026 - elevation`                    | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gebco2026 - slope`                        | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gem - binary`                             | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gem - distance`                           | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalsolaratlas - pvout`                 | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalwindatlas - pd`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalwindatlas - ws`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gold - categorical`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gold - distance`                          | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpm - monthly`                            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpm - yearly`                             | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v3_count`                           | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v3_density`                         | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4_count`                           | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4_density`                         | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4rev11_count`                      | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4rev11_density`                    | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
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
| `petroleum - binary`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `petroleum - distance`                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `plad`                                     | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `pm25 - monthly`                           | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `pm25 - yearly`                            | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `srtm - srtm_elevation_500m`               | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `srtm - srtm_slope_500m`                   | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
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
| `worldpop_pop_count`                       | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `worldpop_pop_count_new`                   | [ ]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
