# WorldPop Population Count by Age and Sex

WorldPop 1km global mosaics of population count broken down by age band and sex ([WorldPop](https://www.worldpop.org/)).

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to process
    - `raw_dir` / `output_dir` are the download and output directories
    - `process_dir` is a working directory for intermediate files
    - `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them

## Important notes

- One raster is produced per sex x age-band x year combination; the full matrix is large, so scope `years` accordingly.
- Processing converts each mosaic to a Cloud Optimized GeoTIFF.
