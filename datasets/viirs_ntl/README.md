# Annual VIIRS Nighttime Lights v2.2 - Average Value

Annual VIIRS nighttime lights product Version 2.2. Average value with background pixels masked. Please use in combination with VIIRS Nighttime Light Cloud Free Coverage product to confirm sufficient cloud free measurements are available within your boundary features.

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `run_annual`
    - `annual_version`
    - `years` is a comma-separated list of years to process
    - `run_monthly`
    - `months`
    - `cf_minimum`
    - `annual_files`
    - `monthly_files`
    - `raw_dir` is a working/output directory
    - `output_dir` is a working/output directory
    - `overwrite_download`, if true, overwrites existing files rather than skipping
    - `overwrite_extract`, if true, overwrites existing files rather than skipping
    - `overwrite_processing`, if true, overwrites existing files rather than skipping
    - `max_retries`
    - `username`
    - `password`
    - `client_secret`

## Source

[Earth Observation Group - VIIRS Nighttime Lights](https://eogdata.mines.edu/products/vnl/)

## Reference

C. D. Elvidge, M. Zhizhin, T. Ghosh, F-C. Hsu, Annual time series of global VIIRS nighttime lights derived from monthly averages: 2012 to 2019, Remote Sensing
