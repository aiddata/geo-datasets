# UCDP GED Conflict Deaths

Prepared the UCDP Georeferenced Event Dataset (GED) global version 26.1 for use in GeoQuery. The dataset contains the number of total fatalities resulting from conflict events. Filterable on pre-defined fields listed in main.py as well as adapted based on outcomes specified in main.py. Intended to be aggregated by year.

Available years are derived from whatever's in the downloaded data each run, not
hardcoded, so new years show up automatically as UCDP releases them.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the download and output directories
- `overwrite_download` / `overwrite_process`, if true, overwrite existing
  files rather than skip them
- `dataset` is the dataset name, used in the output filename and reflects the UCDP GED version

No authentication is required.

## Maintenance note

`download_url` in `main.py` is created based on the config.toml `dataset` variable. If the download url changes in the future, this will need to be adjusted.

## Source

[UCDP Georeferenced Event Dataset](https://ucdp.uu.se/downloads/) — Uppsala
Conflict Data Program.

## Reference

Sundberg, Ralph, and Erik Melander, 2013, "Introducing the UCDP
Georeferenced Event Dataset", Journal of Peace Research, vol.50, no.4,
523-532.

Croicu, Mihai and Ralph Sundberg, "UCDP GED Codebook", Department of Peace
and Conflict Research, Uppsala University.
