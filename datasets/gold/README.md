# Gold Deposits (GOLDATA v1.2)

Large-scale (lootable), small-scale (surface), and non-lootable gold deposit
polygons, rasterized to a global 0.01 degree categorical grid (0 = none,
1 = lootable, 2 = surface, 3 = non-lootable, 4 = mix — a cell covered by more
than one layer), plus a distance-to-nearest-lootable-deposit raster (computed
from the lootable + surface layers only, matching the original script).

## Manual download

ResearchGate blocks automated requests, so this has to be done by hand:

1. Go to
   https://www.researchgate.net/publication/281849073_GOLDATA_12_v and
   download the zip (the codebook, linked from the same page, is useful
   reference but not required by the flow)
2. Extract it and place each layer's shapefile (and sidecar files) at:
   - `<raw_dir>/dGOLD_L/dGOLD_L.shp` (large-scale/lootable)
   - `<raw_dir>/dGOLD_S/dGOLD_S.shp` (small-scale/surface)
   - `<raw_dir>/dGOLD_NL/dGOLD_NL.shp` (non-lootable)

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the input and output directories
- `pixel_size` is the rasterization resolution in degrees
- `overwrite_categorical_raster` / `overwrite_distance_raster`, if true,
  overwrite existing files rather than skip them

## Source

https://www.researchgate.net/publication/281849073_GOLDATA_12_v
