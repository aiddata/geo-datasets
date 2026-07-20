# Gemstone Deposits (GEMDATA 2017-08)

Distance to and presence of gemstone deposits (ruby, sapphire, emerald,
aquamarine, and other gemstones — excludes diamonds), 1022 sites in 61
countries, rasterized to a global 0.01 degree grid.

## Manual download

1. Go to http://www.paivilujala.com/gemdata.html and download the zip
2. Place it at `<raw_dir>/gemdata.zip` (don't unzip it — the flow reads the
   shapefile directly from the zip)

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the input and output directories
- `pixel_size` is the rasterization resolution in degrees
- `overwrite_binary_raster` / `overwrite_distance_raster`, if true, overwrite
  existing files rather than skip them

## Source

http://www.paivilujala.com/gemdata.html — Päivi Lujala, gemstone deposit
dataset.
