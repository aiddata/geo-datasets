# LandScan Population Count

Annual global population count grids from [LandScan](https://landscan.ornl.gov/) (Oak Ridge National Laboratory).

## Authentication

No account or login is required. ORNL's download form (https://landscan.ornl.gov)
submits basic usage info (email, primary use, sector) to a public API endpoint,
which returns a short-lived presigned S3 URL for the requested product/year.
This is replicated directly in `main.py` - just modify `landscan_email`, `landscan_primary_use`, and `landscan_sector` as needed in `config.toml`.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `years` is a comma-separated list of years to process
- `raw_dir` / `output_dir` are the download and output directories
- `run_download` / `overwrite_download` control the download step
- `run_extract` / `overwrite_extract` control the extraction step
- `run_conversion` / `overwrite_conversion` control the COG conversion step
- `landscan_email` / `landscan_primary_use` / `landscan_sector` — usage info submitted with each download request (see Authentication)

## Important notes

- Data is distributed as per-year zip archives that are extracted and converted to Cloud Optimized GeoTIFFs.
