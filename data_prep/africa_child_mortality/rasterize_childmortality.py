
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from distancerasters import rasterize, export_raster


input_path = "/sciclone/aiddata10/REU/geo/raw/africa_child_mortality/ChildMortality5m0Estimates_Burke-HeftNeal-Bendavid_v1.txt"

df = pd.read_table(input_path, sep=' ')
df['geometry'] = df.apply(lambda z: Point(z['lon'], z['lat']), axis=1)
gdf = gpd.GeoDataFrame(df)


pixel_size = 0.1
data_field = 'est5m0'
years = [1980, 1990, 2000]


output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/external/global/africa_child_mortality"

for year in years:
    rst_gdf = gdf[gdf["decade"] == year]
    print "rasterize year", year
    rasterize(
        rst_gdf,
        attribute=data_field,
        pixel_size=pixel_size,
        bounds=rst_gdf.geometry.total_bounds,
        output="{0}/{1}_{2}.tif".format(output_dir, "africa_child_mortality" , year),
        fill=-1,
        nodata=-1)

