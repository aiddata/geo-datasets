# DMSP-like Nighttime Lights Derived from VNL (DVNL)

DMSP-like nighttime lights derived from VIIRS, from the [Earth Observation Group](https://payneinstitute.mines.edu/eog/) (available years 2013-2019).

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to download and process
    - `raw_dir` / `output_dir` are the download and output directories
    - `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them

## Important notes

- Processing converts each annual raster to a Cloud Optimized GeoTIFF.
