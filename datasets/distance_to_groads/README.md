# Distance to Roads (gRoads v1)

Distance (in the units produced by `distancerasters.DistanceRaster`, at a
global 0.01 degree grid) to the nearest road, based on the Global Roads Open
Access Data Set (gROADS) v1 from NASA SEDAC.

The flow downloads the single global gROADS file geodatabase archive,
rasterizes the road network directly from the zip (no separate extraction
step — read via GDAL's `/vsizip/`), and builds a binary road-presence raster
and a distance-to-road raster from it.

## Authentication

The download requires a **NASA Earthdata Login bearer token** (same pattern
as `oco2`/`ltdr_ndvi` — works across Earthdata services).

1. Log in at [https://urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov)
   (register if needed).
2. Generate a token (Earthdata profile → Generate Token) and put it in a
   gitignored `.env` in this directory:
   ```
   earthdata_token=<the token>
   ```
   For a Prefect deployment it is supplied as the `earthdata_token` parameter
   (overlaid from `.env` at deploy time).

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the download and output directories
- `pixel_size` is the rasterization resolution in degrees
- `earthdata_token` — leave the `<ADD-…>` placeholder in `config.toml` and set
  the real value in `.env` (see Authentication)
- `overwrite_download` / `overwrite_binary_raster` / `overwrite_distance_raster`,
  if true, overwrite existing files rather than skip them

## Source

[NASA SEDAC — gROADS v1](https://www.earthdata.nasa.gov/data/catalog/sedac-ciesin-sedac-groads-v1-1.0)

## Reference

Center for International Earth Science Information Network - CIESIN -
Columbia University, and Information Technology Outreach Services - ITOS -
University of Georgia. 2013. Global Roads Open Access Data Set, Version 1
(gROADSv1). Palisades, NY: NASA Socioeconomic Data and Applications Center
(SEDAC). http://dx.doi.org/10.7927/H4VD6WCT.
