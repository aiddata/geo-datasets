# Drug Cultivation Sites (DRUGDATA 2017-08)

Cannabis, coca bush, and opium poppy cultivation site polygons, rasterized to
a global 0.01 degree categorical grid (0 = none, 1 = cannabis, 2 = coca bush,
3 = opium poppy, 4 = mix — a cell covered by more than one type), plus a
distance-to-nearest-cultivation-site raster.

## Manual download

1. Go to http://www.paivilujala.com/drugdata.html and download the zip
2. Place it at `<raw_dir>/drugdata.zip` (don't unzip it — the flow reads each
   layer directly from the zip)

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the input and output directories
- `pixel_size` is the rasterization resolution in degrees
- `overwrite_categorical_raster` / `overwrite_distance_raster`, if true,
  overwrite existing files rather than skip them

## Source

http://www.paivilujala.com/drugdata.html — Päivi Lujala, drug cultivation
dataset.
