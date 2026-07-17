# OCO-2 Carbon Dioxide

OCO-2 Lite Level 2 (Full Physics) column-averaged CO2 (XCO2) from NASA GES DISC. This script downloads the daily `.nc4` lite files, grids and interpolates them, and produces monthly and yearly rasters.

Product page: https://disc.gsfc.nasa.gov/datasets/OCO2_L2_Lite_FP_11.2r/summary

## Authentication

Downloads use a **NASA Earthdata Login bearer token** (works across Earthdata services, GES DISC included).

1. Log in at [https://urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov) (register if needed) and, under Applications → Authorized Apps, authorize the "NASA GESDISC DATA ARCHIVE".
2. Generate a token (Earthdata profile → Generate Token) and put it in a gitignored `.env` in this directory:
   ```
   earthdata_token=<the token>
   ```
   For a Prefect deployment it is supplied as the `earthdata_token` parameter (overlaid from `.env` at deploy time).

## Quick start

Review and edit the variables in `config.toml` as needed:

- `year_list` is a comma-separated list of years to process (earliest complete year is 2015)
- `data_base_url` is the GES DISC path up to the version suffix
- `base_version` / `recent_version` / `recent_start_year` — the OCO2_L2_Lite_FP version to use per year. 11.3r only reprocessed the most recent years, so years `>= recent_start_year` use `recent_version` and earlier years use `base_version`.
- `interp_method` is the interpolation method for the gridding step
- `raw_dir` / `output_dir` are the download and output directories
- `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them
- `earthdata_token` — leave the `<ADD-…>` placeholder in `config.toml` and set the real value in `.env` (see Authentication)
