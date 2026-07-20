# Atlas of Urban Expansion (2016) — 200-city sample

Urban extent boundaries for a global sample of 200 cities, for the study area
and three points in time (`urban_edge_t1` = 1990, `urban_edge_t2` = 2000,
`urban_edge_t3` = 2014), with area/density and blocks/roads metadata attached
to each city as feature properties. From the Atlas of Urban Expansion (NYU,
UN-Habitat, Lincoln Institute of Land Policy).

The flow downloads the two metadata tables and each city's per-city GIS
archive, dissolves each city's boundary into a single (multi)polygon,
reprojects to EPSG:4326, and writes one GeoJSON `FeatureCollection` per level.

## Quick start

Review and edit the variables in `config.toml` as needed:

- `raw_dir` — where source archives and metadata tables are downloaded
- `output_dir` — where the four output GeoJSON files
  (`studyArea.geojson`, `urban_edge_t1.geojson`, `urban_edge_t2.geojson`,
  `urban_edge_t3.geojson`) are written
- `overwrite_download` / `overwrite_process`, if true, overwrite existing
  files rather than skip them

No authentication is required.

## Source

http://www.atlasofurbanexpansion.org — per-city GIS archives and the
Areas & Densities / Blocks & Roads metadata tables.

## Notes

- A handful of city names differ between the metadata tables and the GIS
  archive filenames; these are hardcoded corrections in `main.py`
  (`CITY_NAME_FIXES`, `ZIP_FILENAME_FIXES`) carried over from the original
  script.
- Column reordering (a separate `col_order.py` step in the original,
  tied to the old GeoQuery ingest pipeline) is not needed and was dropped.
