# WorldPop Population Count (1km mosaic)

Estimated population count per 1km pixel, produced by [WorldPop](https://www.worldpop.org/geodata/listing?id=64) using Random Forest-based dasymetric redistribution. Global coverage for the years 2000-2020.

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to download and process
    - `raw_dir` is the directory where the raw GeoTIFFs will be downloaded
    - `output_dir` is the directory where the final COGs will be saved
    - `overwrite_download` / `overwrite_processing`, if true, will overwrite existing files rather than skip them

## Important notes

- Downloads come from `https://data.worldpop.org/GIS/Population/Global_2000_2020/{YEAR}/0_Mosaicked/` (one GeoTIFF per year at 1km resolution).

- Processing converts each raw GeoTIFF to a Cloud Optimized GeoTIFF; pixel values are unchanged from the source data.

- WorldPop's Global 2000-2020 mosaics end at 2020; later years are published under different projects and would need a new URL template.
