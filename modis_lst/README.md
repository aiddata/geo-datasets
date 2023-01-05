# MODIS Land Surface Temperature and Emissivity monthly products

Produces monthly and annual land surface temperature products for day and night readings.

@@ -8,14 +8,64 @@ https://lpdaac.usgs.gov/products/mod11c3v006/
Downloaded from:
https://e4ftl01.cr.usgs.gov/MOLT/MOD11C3.006

Note: MOD11C3.061 is newer, and strongly recommended as of November 2022. We will be updating to it soon.


## Steps:

1. Create an account for (https://urs.earthdata.nasa.gov/)[https://urs.earthdata.nasa.gov/]

2. Review the config.ini file
    - Add your Earthdata username and password (remember not to share these, e.g. in code contributions!)
    - Specify which years you'd like to process as a comma-separated list
    - Pick which run parameters you'd like, in accordance to the Dataset class
