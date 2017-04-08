"""
rasterize ambient air pollution data to 0.1 degree resolution
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from distancerasters import rasterize, export_raster


# -----------------------------------------------------------------------------


input_csv = "/sciclone/aiddata10/REU/raw/ambient_air_pollution/GBD2013final.csv"

pixel_size = 0.1

col_prefixes = ['o3', 'fus_calibrated']

years = [1990, 1995, 2000, 2005, 2010, 2011, 2012, 2013]

output_base = "/sciclone/aiddata10/REU/data/rasters/external/global/ambient_air_pollution"


# -----------------------------------------------------------------------------


df = pd.read_csv(input_csv, delimiter=",", encoding='utf-8')

df['geometry'] = df.apply(lambda z: Point(z['x'], z['y']), axis=1)

gdf = gpd.GeoDataFrame(df)

for pre in col_prefixes:
    for y in years:
        print "rasterizing {0} {1}".format(pre, y)
        rasterize(
            gdf,
            attribute="{0}_{1}".format(pre, y),
            pixel_size=pixel_size,
            bounds=gdf.geometry.total_bounds,
            output="{0}/{1}/{1}_{2}.tif".format(output_base, pre, y))



