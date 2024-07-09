"""
sum of deaths from conflict

UCDP Georeferenced Event Dataset (GED) Global
http://ucdp.uu.se/downloads/


Download to raw directory (include version in path)

Update raw directory source and output in data directory
(with corresponding version in path)
"""

import os
import errno
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from distancerasters import rasterize


# -----------------------------------------------------------------------------


input_path = "/sciclone/aiddata10/REU/geo/raw/ucdp/ged_global_v171/ged171.csv"

output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/ucdp/ged_global_171"

pixel_size = 0.01


# -----------------------------------------------------------------------------


def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def round_point(row):
    """ create a new id to group points that are in near location
    """
    newlon = round(row["longitude"]/pixel_size) * pixel_size
    newlat = round(row["latitude"]/pixel_size) * pixel_size
    newid = str(newlon) + "_" + str(newlat)
    return newid


make_dir(output_dir)

df = pd.read_csv(input_path, sep=',', encoding="utf-8")
df["newid"] = df.apply(lambda row: round_point(row), axis=1)


df['geometry'] = df.apply(
        lambda z: Point(z['longitude'], z['latitude']), axis=1)
gdf = gpd.GeoDataFrame(df)


for year in range(1989, 2017):

    print "Rasterizing year: {0}".format(year)

    filtered = df[df["year"] == year]

    grouped = filtered.groupby(["newid"]).agg(
        {'longitude': 'last', 'latitude': 'last', 'best': 'sum'}).reset_index()

    grouped['geometry'] = grouped.apply(
        lambda z: Point(z['longitude'], z['latitude']), axis=1)

    tmp_gdf = gpd.GeoDataFrame(grouped)

    array, affine = rasterize(
        tmp_gdf,
        attribute="best",
        pixel_size=pixel_size,
        bounds=gdf.geometry.total_bounds,
        output="{0}/ucdp_deaths_{1}.tif".format(output_dir, year),
        fill=0,
        nodata=-1)
