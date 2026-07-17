# GPW Version 4 Revision 11 - Population Count and Density (UN Adjusted)

Population count (UN Adjusted values) from Gridded Population of the World v4 revision 11. GPWv4 depicts the distribution of human population across the globe. Source data provided in 30 arc-second (~1 km) grid cells.

Estimates of population count and density produec by SEDAC for the years 2000, 2005, 2010, 2015, and 2020, consistent with national censuses and population registers with respect to relative spatial distribution, but adjusted to match United Nations country totals.

## Source

[CIESIN](https://www.earthdata.nasa.gov/data/projects/gpw)

Population Count -https://www.earthdata.nasa.gov/data/catalog/sedac-ciesin-sedac-gpwv4-apct-wpp-2015-r11-4.11
Population Density - https://www.earthdata.nasa.gov/data/catalog/sedac-ciesin-sedac-gpwv4-apdens-wpp-2015-r11-4.11

## Quick start


1. [Create EarthData login for LAADS](https://urs.earthdata.nasa.gov/users/new)

2. Generate a token:
   - Navigate to the [LAADS DAAC website](https://ladsweb.modaps.eosdis.nasa.gov/)
   - Click on "Login" at the top right of the screen
   - Click on "Generate Token"
   - Copy the generated token into `.env` file in the `datasets/gpw` directory using the format `EARTHDATA_TOKEN=your_token_here`

3. Review and edit the variables in `config.toml` as needed
    - `name`
    - `raw_dir` is a working/output directory
    - `output_dir` is a working/output directory
    - `years` is a comma-separated list of years to process
    - `sedac_cookie`
    - `overwrite_download`, if true, overwrites existing files rather than skipping
    - `overwrite_extract`, if true, overwrites existing files rather than skipping
    - `overwrite_processing`, if true, overwrites existing files rather than skipping

https://data.earthdata.nasa.gov/nasa-earth/human-dimensions/sedac-root/downloads/data/gpw-v4/gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11/gpw-v4-population-density-adjusted-to-2015-unwpp-country-totals-rev11_2015_1_deg_tif.zip

## Reference

Center For International Earth Science Information Network-CIESIN-Columbia University. (2018). Gridded Population of the World, Version 4 (GPWv4): Population Count Adjusted to Match 2015 Revision of UN WPP Country Totals, Revision 11 (Version 4.11) [Data set]. Palisades, NY: NASA Socioeconomic Data and Applications Center (SEDAC). https://doi.org/10.7927/H4PN93PB Date Accessed: 2026-07-17

Center For International Earth Science Information Network-CIESIN-Columbia University. (2018). Gridded Population of the World, Version 4 (GPWv4): Population Density Adjusted to Match 2015 Revision UN WPP Country Totals, Revision 11 (Version 4.11) [Data set]. Palisades, NY: NASA Socioeconomic Data and Applications Center (SEDAC). https://doi.org/10.7927/H4F47M65 Date Accessed: 2026-07-17
