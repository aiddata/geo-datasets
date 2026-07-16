# Yearly Normalized Difference Vegetation Index - NDVI (LTDR v5 - AVHRR)

Yearly value for Normalized Difference Vegetation Index (NDVI). Created using the NASA Long Term Data Record (v5) AVHRR data.

## Quick start

1. Review and edit the variables in `config.toml` as needed
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
