# WWLLN Global Lightning Climatology (Density)

Global monthly lightning stroke density, derived from the World Wide
Lightning Location Network ([WWLLN](https://wwlln.net/)).

**Source:** [zenodo.org/records/20277101](https://zenodo.org/records/20277101)
(DOI: [10.5281/zenodo.20277101](https://doi.org/10.5281/zenodo.20277101))

## About the source data

WWLLN's raw stroke-level archival data is copyrighted/paywalled (available
for purchase from the University of Washington). This flow instead uses the
**WWLLN Global Lightning Climatology (WGLC)**, a freely-licensed (CC BY-SA
4.0) derived product: WWLLN's raw strokes, reprocessed and gridded into
density rasters, corrected for detection efficiency.

The Zenodo record publishes several files at different resolutions/temporal
aggregations - this flow only downloads and processes
**`wglc_timeseries_05m_monthly.nc`** (5 arc-minute monthly density, 2010 to
present). Notes on scope:

- The 5 arc-minute release only includes the `density` variable. Power
  statistics (`power_mean`/`power_median`/`power_SD`) are only published at
  30 arc-minute resolution, in a separate file this flow doesn't process.
- The 30 arc-minute daily file (5,479 timesteps) is also not processed here.

If those are needed later, `download_url`/`download_filename`/`expected_md5`
in `config.toml` would need to point at a different file, and
`build_process_list()` would need updating for a different variable/band
count (see the [Zenodo record's file list](https://zenodo.org/records/20277101)
for the other options and their MD5 checksums).

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the download and output directories
- `download_url` / `download_filename` / `expected_md5` identify the source
  file (Zenodo's URLs end in `/content`, so the real filename can't be
  derived from the URL and is given explicitly; the MD5 is verified after
  every download)
- `year_agg_method` - how to combine a year's 12 monthly rasters into the
  annual output: `sum` (default), `mean`, `max`, or `min`
- `overwrite_download` / `overwrite_processing`, if true, overwrite existing
  files rather than skipping them

## Pipeline

1. **Download** - fetches the source NetCDF, verifying it against the
   published MD5 checksum
2. **Monthly** - extracts each monthly band (192 as of the 2026 release,
   Jan 2010 - Dec 2025) to a COG in `output_dir/monthly/`, named
   `wglc_density_<year>_<month>.tif`. Band-to-date mapping is read from the
   file's own `time` variable (CF `days since 2010-01-01`), not assumed
   from position.
3. **Yearly** - aggregates each complete year's 12 monthly COGs (via
   `year_agg_method`) into an annual COG in `output_dir/yearly/`, named
   `wglc_density_<year>.tif`. Years with fewer than 12 monthly files (e.g.
   an in-progress current year) are skipped.

## Citation

Kaplan, J. O., & Lau, K. H.-K. (2022). World Wide Lightning Location Network
(WWLLN) Global Lightning Climatology (WGLC) and time series, 2022 update.
*Earth System Science Data*, 14(12), 5665-5670.
[doi:10.5194/essd-14-5665-2022](https://doi.org/10.5194/essd-14-5665-2022)

Kaplan, J. O., & Lau, K. H.-K. (2021). The WGLC global gridded lightning
climatology and time series. *Earth System Science Data*, 13(7), 3219-3237.
[doi:10.5194/essd-13-3219-2021](https://doi.org/10.5194/essd-13-3219-2021)
