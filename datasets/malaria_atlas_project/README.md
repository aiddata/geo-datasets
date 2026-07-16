# Malaria Atlas - PF Incidence

Plasmodium falciparum incidence rasters from the [Malaria Atlas Project](https://data.malariaatlas.org).

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `dataset` selects the Malaria Atlas data product to download
    - `years` is a comma-separated list of years to process
    - `raw_dir` / `output_dir` are the download and output directories
    - `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them

## Important notes

- Data is retrieved through the Malaria Atlas Project API and converted to Cloud Optimized GeoTIFFs.
