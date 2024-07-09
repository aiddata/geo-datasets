#!/bin/bash

src=/sciclone/aiddata10/REU/geo/raw/accessibility_to_cities_2015_v1.0/accessibility_to_cities_2015_v1.0.tif
dst=/sciclone/aiddata10/REU/geo/data/rasters/accessibility_to_cities_2015_v1.0/accessibility_to_cities_2015_v1.0.tif
mkdir -p $(dirname ${dst})

gdal_translate -a_nodata -9999 -of GTiff -co TILED=YES -co BIGTIFF=YES ${src} ${dst}
