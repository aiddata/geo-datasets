
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from distancerasters import rasterize

# input_path = r"/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/ucdp-ged-poly-v-1-1-shape/UCDPGEDpolyyear.shp"

input_path = "/sciclone/home10/zlv/datasets/ucdp/ged171.csv"

df = pd.read_csv(input_path, sep=',', encoding="utf-8")
df['geometry'] = df.apply(lambda z: Point(z['longitude'], z['latitude']), axis=1)
gdf = gpd.GeoDataFrame(df)


pixel_size = 0.1
data_field = 'best' # best estimate of deaths

field_values = {1: "State-based", 2: "Non-state", 3: "One-sided"}

# output_dir = "/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/output"

output_dir = "/sciclone/data20/zlv/ucdp/fatality"

for field in field_values.keys():

    field_gdf = gdf[gdf["type_of_violence"] == field]

    for year in range(1989, 2016):

        rst_gdf = field_gdf[field_gdf["year"] == year]

        print "rasterize year", year

        rasterize(
            rst_gdf,
            attribute=data_field,
            pixel_size=pixel_size,
            bounds=rst_gdf.geometry.total_bounds,
            output="{0}/{1}_{2}_{3}.tif".format(output_dir, "ucdp_fatality", field_values[field], year),
            fill=-1,
            nodata=-1)