# LandScan Population Count

Annual global population count grids from [LandScan](https://landscan.ornl.gov/) (Oak Ridge National Laboratory).

## Quick start

1. A LandScan account is required to download the data; register at https://landscan.ornl.gov/ and place credentials as expected by the download step.

2. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to process
    - `raw_dir` / `output_dir` are the download and output directories
    - `run_extract` / `overwrite_extract` control the extraction step

## Important notes

- Data is distributed as per-year zip archives that are extracted and converted to Cloud Optimized GeoTIFFs.
