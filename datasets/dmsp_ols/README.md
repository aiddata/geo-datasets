# DMSP-OLS Nighttime Lights

Version 4 DMSP-OLS nighttime lights composites from the [Earth Observation Group (EOG)](https://eogdata.mines.edu/products/dmsp/) at Colorado School of Mines (satellite-years 1992-2013). The final product is the stable-lights composite inter-satellite calibrated across sensors/years using the Elvidge-2014 coefficients.

Two EOG source sections are downloaded:
- **v4 composites** — the raw `stable_lights.avg_vis` rasters, which are then calibrated (Elvidge 2014) into the final Cloud Optimized GeoTIFFs.
- **avg_lights_x_pct** — the `.tgz` archives, downloaded and extracted only (no output product built).

## Authentication

EOG moved programmatic access behind a paid tier, so downloads now use a browser
**session cookie**. Because the session times out quickly when idle, a
background thread pings EOG every 30 seconds to keep it warm during a run — so
**grab the cookie immediately before running**.

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

Review and edit the variables in `config.toml` as needed:

- `run_composites` — download the composites and build the calibrated product
- `run_avg_x_pct` — download + extract the avg_lights_x_pct archives
- `raw_dir` / `output_dir` are the download and output directories
- `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them
- `mod_auth_openidc_session` — the EOG cookie; leave the `<ADD-…>` placeholder in `config.toml` and set the real value in `.env` (see Authentication)

## Notes

- The satellite-year selection is fixed in `main.py` (`COMPOSITE_SATYEARS`, `AVG_X_PCT_SATYEARS`) since DMSP v4 is a completed historical archive. The composite version suffix (v4b/v4c/v4d) varies per satellite-year, so the composite download discovers the file by listing the directory; the avg_x_pct suffix is predictable and constructed directly.
- Calibration: `dn_adjusted = c0 + c1·dn + c2·dn²`, capped at 63; background (0) stays 0 and the 255 nodata value is preserved. Coefficients live in `intercalibration_coefficients.py`.
- The previous shell download scripts and the Python-2 `processing.py` are kept in `archive/`.

## Reference

Elvidge, Christopher D., Feng-Chi Hsu, Kimberly E. Baugh, and Tilottama Ghosh. "National trends in satellite-observed lighting." Global urban monitoring and assessment through earth observation 23 (2014): 97-118.
