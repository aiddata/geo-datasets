# Distance to Coast


Distance to coast (on land only), measured in meters. Derived using World Vector Shorelines from [GSHHG](http://www.soest.hawaii.edu/pwessel/gshhg/).

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `raw_dir` is a working/output directory
    - `output_dir` is a working/output directory
    - `gshhg_version`
    - `overwrite_download`, if true, overwrites existing files rather than skipping
    - `overwrite_extract`, if true, overwrites existing files rather than skipping
    - `overwrite_binary_raster`, if true, overwrites existing files rather than skipping
    - `overwrite_distance_raster`, if true, overwrites existing files rather than skipping
    - `pixel_size`
    - `download_dest`
    - `raster_type`

## Source

[GSHHG: A Global Self-consistent, Hierarchical, High-resolution Geography Database](http://www.soest.hawaii.edu/pwessel/gshhg/)

## Reference

Wessel, P., and W. H. F. Smith, A Global Self-consistent, Hierarchical, High-resolution Shoreline Database, J. Geophys. Res., 101, #B4, pp. 8741-8743, 1996.
