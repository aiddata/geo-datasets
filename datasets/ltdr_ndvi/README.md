# Yearly Normalized Difference Vegetation Index - NDVI (LTDR v5 - AVHRR)

Yearly value for Normalized Difference Vegetation Index (NDVI). Created using the NASA Long Term Data Record (v5) AVHRR data.

[LTDR (Long-Term Data Record)](https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/applications/ltdr/) is a project at NASA that "produces, validates and distributes a climate data record." [NDVI (Normalized Difference Vegetation Index)](https://modis-land.gsfc.nasa.gov/vi.html) "provides continuity with NOAA's AVHRR NDVI time series record for historical and climate applications."
This script downloads daily NDVI data, unpacks them from HDF containers into the GeoTIFF format, and create monthly and yearly aggregates.

## Instructions

1. [Create EarthData login for LAADS](https://urs.earthdata.nasa.gov/users/new)

2. Generate a token:
   - Navigate to the [LAADS DAAC website](https://ladsweb.modaps.eosdis.nasa.gov/)
   - Click on "Login" at the top right of the screen
   - Click on "Generate Token"
   - Copy the generated token into `.env` file in the `datasets/ltdr_ndvi` directory using the format `EARTHDATA_TOKEN=your_token_here`

3. Review and edit the variables in `config.toml` as needed
    - `data_num`
    - `years` is a comma-separated list of years to process
    - `raw_dir` is a working/output directory
    - `output_dir` is a working/output directory
    - `token`
    - `overwrite_download`, if true, overwrites existing files rather than skipping
    - `validate_download`
    - `overwrite_processing`, if true, overwrites existing files rather than skipping

## Source

[NASA LAADS DAAC](https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/applications/ltdr/)

## Reference

Pedelty JA, Devadiga S, Masuoka E et al. (2007) Generating a Long-term Land Data Record from the AVHRR and MODIS Instruments. Proceedings of IGARRS 2007, pp. 1021–1025. Institute of Electrical and Electronics Engineers, NY, USA.
