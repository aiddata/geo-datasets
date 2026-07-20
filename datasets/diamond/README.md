# Diamond Resources (DIADATA)

Distance to and presence of diamond deposits, rasterized to a global 0.01
degree grid, from PRIO's Diamond Resources dataset (1946-2004, global
coverage).

## Manual download

PRIO's download link redirects through an organizational (Microsoft Entra)
login and can't be fetched programmatically:

1. Go to https://www.prio.org/data/10 and download the data zip
2. Extract it and place `DIADATA.shp` (and its sidecar files — `.dbf`, `.shx`,
   `.prj`, etc.) directly in `<raw_dir>/`

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` / `output_dir` are the input and output directories
- `pixel_size` is the rasterization resolution in degrees
- `overwrite_binary_raster` / `overwrite_distance_raster`, if true, overwrite
  existing files rather than skip them

## Source

[PRIO — Diamond Resources](https://www.prio.org/data/10)
