# Petroleum Deposits (PETRODATA v1.2, onshore)

Distance to and presence of onshore petroleum deposits, rasterized to a
global 0.01 degree grid, from PRIO's Petroleum Dataset v1.2. Onshore only —
PRIO also publishes an offshore layer, not covered here (matches the original
script's scope).

## Manual download

PRIO's download link redirects through an organizational (Microsoft Entra)
login and can't be fetched programmatically:

1. Go to https://www.prio.org/data/11 and download the data zip
2. Extract it and place `Petrodata_Onshore_V1.2.shp` (and its sidecar files —
   `.dbf`, `.shx`, `.prj`, etc.) directly in `<raw_dir>/`

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the input and output directories
- `pixel_size` is the rasterization resolution in degrees
- `overwrite_binary_raster` / `overwrite_distance_raster`, if true, overwrite
  existing files rather than skip them

## Source

[PRIO — Petroleum Dataset v1.2](https://www.prio.org/data/11)
