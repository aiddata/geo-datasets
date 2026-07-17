# Annual VIIRS Nighttime Lights v2.2 - Average Value

Annual VIIRS nighttime lights product Version 2.2. Average value with background pixels masked. Please use in combination with VIIRS Nighttime Light Cloud Free Coverage product to confirm sufficient cloud free measurements are available within your boundary features.

[The Visible Infrared Imaging Radiometer Suite (VIIRS)](https://ncc.nesdis.noaa.gov/VIIRS/) is an instrument on a National Oceanic and Atmospheric Administation (NOAA) satellite that collects atmospheric imagery.
The [Earth Observation Group (EOG)](https://payneinstitute.mines.edu/eog/) at Colorado School of Mines produces monthly and annual composites of nighttime lights, using data from VIIRS.

Product abbreviations:
	- viirs = Visible Infrared Imaging Radiometer Suite
	- dnb = Day/Night Band
	- vcm = viirs cloud mask
	- sl = stray light
	- cfg = cloud free grids
	- vcmcfg = excludes any data contaminated by stray light
	- vcmslcfg = data impacted by stray light are corrected but not removed

## Authentication

EOG moved programmatic (OAuth) access behind a paid tier, so downloads now use a
browser **session cookie** instead. Because that session times out quickly when
idle, a background thread pings EOG every 30 seconds to keep it warm for the
duration of a run — so **grab the cookie immediately before running**, not ahead
of time.

1. Log in at [https://eogdata.mines.edu/nighttime_light](https://eogdata.mines.edu/nighttime_light) (register [here](https://eogdata.mines.edu/eog/EOG_sensitive_contents) if needed).
2. In your browser's DevTools → Application → Cookies → `eogdata.mines.edu`, copy the value of the `mod_auth_openidc_session` cookie.
3. Put it in a gitignored `.env` in this directory:
   ```
   mod_auth_openidc_session=<the cookie value>
   ```
   For a Prefect deployment it is supplied as the `mod_auth_openidc_session` parameter (overlaid from `.env` at deploy time).

Note: the cookie's server-side session expires after a period of inactivity that
EOG controls (kept warm by the keep-alive during a run). If a run fails with a
"redirected to login" error, the cookie has expired — grab a fresh one and rerun.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `run_annual` / `run_monthly` toggle the two products
- `annual_version` selects the annual release (e.g. `v22`)
- `years` / `months` are comma-separated lists to process
- `annual_files` / `monthly_files` are comma-separated file types to fetch
- `cf_minimum` is the cloud-free-coverage threshold used to binarize the `cf_cvg` product
- `raw_dir` / `output_dir` are the download and output directories
- `overwrite_download` / `overwrite_extract` / `overwrite_processing`, if true, overwrite existing files rather than skipping
- `max_retries` is the retry count for the monthly directory listing
- `mod_auth_openidc_session` — the EOG cookie; leave the `<ADD-…>` placeholder in `config.toml` and set the real value in `.env` (see Authentication)

## Source

[Earth Observation Group - VIIRS Nighttime Lights](https://eogdata.mines.edu/products/vnl/)

## References

Any VNL
C. D. Elvidge, K. E. Baugh, M. Zhizhin, and F.-C. Hsu, “Why VIIRS data are superior to DMSP for mapping nighttime lights,” Asia-Pacific Advanced Network 35, vol. 35, p. 62, 2013.

Annual VNL V2
C. D. Elvidge, M. Zhizhin, T. Ghosh, F-C. Hsu, "Annual time series of global VIIRS nighttime lights derived from monthly averages: 2012 to 2019", Remote Sensing (In press)
