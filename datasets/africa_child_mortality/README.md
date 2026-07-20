# Africa Child Mortality

Under-5 mortality (estimated deaths per 1,000 child-years) for the 1980s,
1990s, and 2000s, based on geospatial interpolation of Demographic and Health
Survey data across 28 Sub-Saharan countries (Burke, Heft-Neal & Bendavid,
2016). Point estimates are downloaded as a single text file and rasterized
per decade at 0.1 degree resolution.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` — where the source point data is downloaded
- `output_dir` — where the output rasters (`africa_child_mortality_<decade>.tif`)
  are written
- `overwrite_download` / `overwrite_process`, if true, overwrite existing files
  rather than skip them

No authentication is required.

## Source

Point data is hosted on Dropbox; the original project page
(sheftneal9.wixsite.com/fse-data) is no longer reliably available, so the
flow downloads directly from the last known-working Dropbox share link.

## Reference

Marshall Burke, Sam Heft-Neal, and Eran Bendavid. Understanding variation in
child mortality across Sub-Saharan Africa: A spatial analysis. The Lancet
Global Health, 2016, Volume 4, Issue 12, e936-e945.
