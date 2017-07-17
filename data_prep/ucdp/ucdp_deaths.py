
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from distancerasters import rasterize

input_path = r"/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/ged171.csv"
# input_path = "/sciclone/home10/zlv/datasets/ucdp/ged171.csv"

df = pd.read_csv(input_path, sep=',', encoding="utf-8")
df['geometry'] = df.apply(lambda z: Point(z['longitude'], z['latitude']), axis=1)
gdf = gpd.GeoDataFrame(df)


pixel_size = 0.01

output_dir = "/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/output"
# output_dir = "/sciclone/data20/zlv/ucdp/fatality"


# create a new id to group points that are in near location

def round_point(row):

    newlat = int(row["latitude"]/pixel_size) * pixel_size

    newlon = int(row["longitude"]/pixel_size) * pixel_size

    newid = str(newlat) + "_" + str(newlon)

    return newid


gdf["newid"] = gdf.apply(lambda row: round_point(row), axis=1)


for year in range(1989, 2017):

    filtered = gdf[gdf["year"] == year]

    groups = filtered.groupby(["newid"])["best"].sum().reset_index()

    groups = groups.rename(columns={"best":"sum_best"})

    rst_gdf = filtered.merge(groups,how="left", on="newid")

    print "rasterize year", year

    rasterize(
        rst_gdf,
        attribute="sum_best",
        pixel_size=pixel_size,
        bounds=rst_gdf.geometry.total_bounds,
        output="{0}/{1}_{2}.tif".format(output_dir, "ucdp_fatality", year),
        fill=-1,
        nodata=-1)
