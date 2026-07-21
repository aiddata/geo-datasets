# UCDP GED Conflict Deaths

Sum of the "best" fatality estimate per 0.01 degree grid cell per year, from
the UCDP Georeferenced Event Dataset (GED) — both combined across all
violence types and broken out by type (state-based, non-state, one-sided).
One flow, one download, produces all four variants:

- `all/ucdp_deaths_all_<year>.tif`
- `state-based/ucdp_deaths_state-based_<year>.tif`
- `non-state/ucdp_deaths_non-state_<year>.tif`
- `one-sided/ucdp_deaths_one-sided_<year>.tif`

Years are derived from whatever's in the downloaded data each run, not
hardcoded, so new years show up automatically as UCDP releases them.

## Dropped: UCDP GED Polygons (binary conflict-occurrence raster)

An older script in this directory (now removed) built a binary
conflict-occurrence raster from the UCDP GED Polygons dataset (v1.1, 2012).
That dataset is no longer listed on https://ucdp.uu.se/downloads/ and
appears to have been retired, so this output was dropped rather than
migrated.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the download and output directories
- `overwrite_download` / `overwrite_process`, if true, overwrite existing
  files rather than skip them

No authentication is required.

## Maintenance note

`DOWNLOAD_URL` in `main.py` is pinned to the current GED release (v26.1).
UCDP releases a new version roughly annually (with a new version number in
the URL and the zipped CSV's filename) — bump the URL when they do.

## Source

[UCDP Georeferenced Event Dataset](https://ucdp.uu.se/downloads/) — Uppsala
Conflict Data Program.

## Reference

Sundberg, Ralph, and Erik Melander, 2013, "Introducing the UCDP
Georeferenced Event Dataset", Journal of Peace Research, vol.50, no.4,
523-532.

Croicu, Mihai and Ralph Sundberg, "UCDP GED Codebook", Department of Peace
and Conflict Research, Uppsala University.
