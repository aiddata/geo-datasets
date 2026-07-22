# MODIS Land Cover (MCD12Q1 v061)

Annual global land cover at 500m resolution from combined Terra+Aqua MODIS observations. Uses IGBP classification (LC_Type1).

**Product:** MCD12Q1 v061  
**CMR concept:** C2484079608-LPCLOUD  
**Coverage:** 2001–present  
**Source:** [LP DAAC](https://lpdaac.usgs.gov/products/mcd12q1v061/)

## Setup

Add your [Earthdata bearer token](https://urs.earthdata.nasa.gov) to `.env`:

```
earthdata_token=<your-token>
```

Edit `config.toml` to set `raw_dir`, `process_dir`, `output_dir`, and `years`.

## Pipeline

1. **CMR search** — queries `C2484079608-LPCLOUD` for all HDF4 tiles per year (~317 land tiles/year)
2. **Download** — fetches `.hdf` granules from LP DAAC with Bearer token auth
3. **Tile extraction** — `gdal_translate` extracts the `LC_Type1` subdataset from each HDF4 tile to GeoTIFF
4. **Mosaic & reproject** — `gdal_merge.py` mosaics tiles in MODIS sinusoidal projection; `gdalwarp` reprojects to WGS84 (nearest-neighbor to preserve integer class values)

Output files: `mcd12q1_061_lc_type1_{year}.tif`

## Run

```bash
python main.py
```

## Citation

Friedl, M., Sulla-Menashe, D. (2022). MODIS/Terra+Aqua Land Cover Type Yearly L3 Global 500m SIN Grid V061. NASA EOSDIS Land Processes Distributed Active Archive Center. https://doi.org/10.5067/MODIS/MCD12Q1.061
