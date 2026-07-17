# DMSP-like Nighttime Lights Derived from VNL (DVNL)

DMSP-like nighttime lights derived from VIIRS, from the [Earth Observation Group](https://payneinstitute.mines.edu/eog/) (available years 2013-2019).

## Authentication

EOG (eogdata.mines.edu) moved programmatic access behind a paid tier, so
downloads now use a browser **session cookie**. Because the session times out
quickly when idle, a background thread pings EOG every 30 seconds to keep it
warm during a run — so **grab the cookie immediately before running**.

1. Log in at [https://eogdata.mines.edu](https://eogdata.mines.edu).
2. In DevTools → Application → Cookies → `eogdata.mines.edu`, copy the value of the `mod_auth_openidc_session` cookie.
3. Put it in a gitignored `.env` in this directory:
   ```
   mod_auth_openidc_session=<the cookie value>
   ```
   For a Prefect deployment it is supplied as the `mod_auth_openidc_session` parameter (overlaid from `.env` at deploy time).

If a run fails with a "redirected to login" error, the cookie has expired —
grab a fresh one and rerun.

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to download and process
    - `mod_auth_openidc_session` — the EOG cookie; leave the `<ADD-…>` placeholder in `config.toml` and set the real value in `.env` (see Authentication)
    - `raw_dir` / `output_dir` are the download and output directories
    - `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them

## Important notes

- Processing converts each annual raster to a Cloud Optimized GeoTIFF.
