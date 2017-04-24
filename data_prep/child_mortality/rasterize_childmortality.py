
import pandas
import os
from os.path import join
from shapely.geometry import Point
import geopandas as gpd
from distancerasters import rasterize, export_raster

# read file
path = os.path.dirname(os.path.realpath(__file__))
file_nm = "ChildMortality5m0Estimates_Burke-HeftNeal-Bendavid_v1.txt"
file_path = join(path, file_nm)

df = pandas.read_table(file_path, sep=' ')

years = [1980, 1990, 2000]
pixel_size = 0.1
#raster_field = 'est5m0'

df['geometry'] = df.apply(lambda z: Point(z['lon'], z['lat']), axis=1)

gdf = gpd.GeoDataFrame(df)


for year in years:
    rst_gdf = gdf[gdf["decade"]==year]
    print "rasterize year", year
    rasterize(
        rst_gdf,
        attribute='est5m0',
        pixel_size=pixel_size,
        bounds=rst_gdf.geometry.total_bounds,
        output="{0}/{1}_{2}.tif".format(path, "ChildMortality" ,year),
        fill=-1,
        nodata=-1)

