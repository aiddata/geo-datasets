# CRU TS

CRU TS (Climatic Research Unit gridded Time Series) is a dataset of monthly climate variables on a 0.5x0.5° grid covering the entire world except Antarctica, spanning 1901-present and updated annually. See the [data source](https://crudata.uea.ac.uk/cru/data/hrg/).

These scripts download and unzip CRU TS data, convert each month from NetCDF to COG, and create yearly aggregates.

## Quick start

1. Set the CRU TS version in `config.toml` (`cru_url_dir`) — see below if updating to a new release.

2. Review and edit the variables in `config.toml` as needed
    - `start_year` / `end_year` set the range to process (all years are downloaded regardless)
    - `raw_dir` / `output_dir` are the download and output directories
    - `overwrite_download` / `overwrite_unzip` / `overwrite_processing`, if true, overwrite existing files rather than skip them

## Important notes

- The download links differ between versions, so a new release may require revising the link construction in `CRU_TS.download()` in `main.py`.

- Previous (pre-`Dataset`-class) versions of this code are in the `archive/` folder.

## Reference

Harris, I., Osborn, T.J., Jones, P. et al. Version 4 of the CRU TS monthly high-resolution gridded multivariate climate dataset. Sci Data 7, 109 (2020). https://doi.org/10.1038/s41597-020-0453-3
