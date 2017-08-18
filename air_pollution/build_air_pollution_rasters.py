"""
rasterize ambient air pollution data to 0.1 degree resolution
"""

import itertools
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from distancerasters import rasterize, export_raster


# -----------------------------------------------------------------------------


input_csv = "/sciclone/aiddata10/REU/raw/ambient_air_pollution_2013/GBD2013final.csv"

pixel_size = 0.1

col_prefixes = ['o3', 'fus_calibrated']

years = [1990, 1995, 2000, 2005, 2010, 2011, 2012, 2013]

output_base = "/sciclone/aiddata10/REU/data/rasters/external/global/ambient_air_pollution_2013"


# -----------------------------------------------------------------------------

field_list = ["{0}_{1}".format(*i) for i in itertools.product(col_prefixes, years)]

df = pd.read_csv(input_csv, delimiter=",", encoding='utf-8')

df = df[field_list + ['x', 'y']]

df['geometry'] = df.apply(lambda z: Point(z['x'], z['y']), axis=1)

gdf = gpd.GeoDataFrame(df)


for field in field_list:
    print "rasterizing {0}".format(field)
    rasterize(
        gdf,
        attribute=field,
        pixel_size=pixel_size,
        bounds=gdf.geometry.total_bounds,
        output="{0}/{1}/{2}.tif".format(output_base, field[:-5], field),
        fill=-1,
        nodata=-1)



