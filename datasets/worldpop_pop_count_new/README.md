# WorldPop Population Count (1km mosaic, 2015-2030 R2025A)

Estimated population count per 1km pixel from the [WorldPop global mosaics 2015-2030, release R2025A v1](https://hub.worldpop.org/geodata/listing?id=137). Constrained counts mosaiced from the 100m resolution country datasets; global coverage for the years 2015-2030.

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to download and process
    - `un_adjusted`, if true, downloads the UN-adjusted (UA) variant of the mosaics
    - `raw_dir` is the directory where the raw GeoTIFFs will be downloaded
    - `output_dir` is the directory where the final COGs will be saved
    - `overwrite_download` / `overwrite_processing`, if true, will overwrite existing files rather than skip them

## Important notes

- Downloads come from `https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{YEAR}/0_Mosaicked/v1/` (one GeoTIFF per year at 1km resolution; the UA variant lives under `1km_ua/` with a `_UA` suffix in the filename).

- Years past the release date are projections, and earlier years are modelled estimates — see the [release statement](https://hub.worldpop.org/geodata/listing?id=137) for methodology.

- Processing converts each raw GeoTIFF to a Cloud Optimized GeoTIFF; pixel values are unchanged from the source data.

- This supersedes the Global 2000-2020 unconstrained dataset (`worldpop_pop_count`); note the new release is **constrained**, so values are not directly comparable with the old series.
