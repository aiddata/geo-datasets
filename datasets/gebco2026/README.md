# GEBCO 2026 Grid — elevation and slope

Global land and ice surface elevation (meters; negative values are ocean
depth/bathymetry) at 15 arc-second (~450m) resolution, from the GEBCO_2026
Grid, plus a derived global slope raster (degrees).

GEBCO distributes the grid as 8 quadrant GeoTIFFs (90°x90° each) in a single
zip. The flow downloads that zip, mosaics the 8 tiles into one seamless
global elevation raster, and computes slope from the mosaic using Horn's
method with a latitude-dependent correction for the varying real-world width
of a degree of longitude (needed here because — unlike a small per-tile DEM —
this raster spans the full globe, where that width varies enormously between
the equator and high latitudes).

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` — where the source zip (~4.2GB) is downloaded
- `output_dir` — where `gebco2026_elevation.tif` and `gebco2026_slope.tif`
  are written
- `overwrite_download` / `overwrite_elevation` / `overwrite_slope`, if true,
  overwrite existing files rather than skip them

No authentication is required.

## Source

[GEBCO_2026 Grid](https://www.gebco.net/data-products-gridded-bathymetry-data/gebco2026-grid)

## Citation

GEBCO Compilation Group (2026) GEBCO_2026 Grid (doi:10.5285/gebco2026-grid).
