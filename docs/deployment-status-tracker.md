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
| `cru_ts - monthly_pre`                     | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `cru_ts - monthly_tmp`                     | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `cru_ts - yearly_pre`                      | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `cru_ts - yearly_tmp`                      | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `diamond - binary`                         | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `diamond - distance`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_coast`                        | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_country_border`               | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `distance_to_groads`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `distance_to_water`                        | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `dmsp_ols`                                 | [?]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `drug - categorical`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `drug - distance`                          | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `dvnl`                                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `esa_landcover`                            | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gcdf_v3_dynamic`                          | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gebco2026 - elevation`                    | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `gebco2026 - slope`                        | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `gem - binary`                             | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gem - distance`                           | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalsolaratlas - pvout`                 | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalwindatlas - pd`                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `globalwindatlas - ws`                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gold - categorical`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gold - distance`                          | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpm - monthly`                            | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `gpm - yearly`                             | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `gpw - v3_count`                           | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v3_density`                         | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4_count`                           | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4_density`                         | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4rev11_count`                      | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `gpw - v4rev11_density`                    | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `landscan_pop`                             | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `ltdr_ndvi - v6_monthly`                   | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `ltdr_ndvi - v6_yearly`                    | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `map - motorized_healthcare_2020`          | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `map - pf_incidence_rate`                  | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `map - cities_2015`                        | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `map - walking_healthcare_2020`            | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `modis_landcover`                          | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/monthly_day`              | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/monthly_night`            | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/yearly_day`               | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `modis_lst - 061/yearly_night`             | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `oco2 - monthly_xco2`                      | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `oco2 - yearly_xco2`                       | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `petroleum - binary`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `petroleum - distance`                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `plad`                                     | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `pm25 - monthly`                           | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `pm25 - yearly`                            | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `srtm - srtm_elevation_500m`               | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `srtm - srtm_slope_500m`                   | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `ucdp - ged261_filter_ingest.json`         | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `udel_climate - air_temp_2017_yearly_mean` | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `udel_climate - precip_2017_yearly_mean`   | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `udel_climate - precip_2017_yearly_sum`    | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `viirs - annual cf cvg`                    | [ ]      | [ ]     | [R] | [ ]      | [ ]  | [ ]      |
| `viirs - annual avg`                       | [ ]      | [ ]     | [R] | [ ]      | [ ]  | [ ]      |
| `viirs - monthly cf cvg`                   | [ ]      | [ ]     | [R] | [ ]      | [ ]  | [ ]      |
| `viirs - monthly avg`                      | [ ]      | [ ]     | [R] | [ ]      | [ ]  | [ ]      |
| `wdpa - wdpa_iucn`                         | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
| `worldpop_pop_count`                       | [c]      | [ ]     | [ ] | [ ]      | [ ]  | [ ]      |
| `worldpop_pop_count_new`                   | [ ]      | [ ]     | [c] | [ ]      | [ ]  | [ ]      |
