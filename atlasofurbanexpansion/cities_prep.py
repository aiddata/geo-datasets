"""
http://www.atlasofurbanexpansion.org
"""


import os
import errno
import glob
import copy
import urllib2
import zipfile
import json

import fiona
import pandas as pd
import numpy as np
from shapely.geometry import mapping, shape, MultiPolygon
from shapely.ops import cascaded_union

from functools import partial
import pyproj
from shapely.ops import transform

def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


base_dir = "/sciclone/aiddata10/REU/scr/atlasofurbanexpansion"


metadata_path = os.path.join(base_dir, "metadata.csv")


raw_dir = os.path.join(base_dir, "raw")
shps_dir = os.path.join(base_dir, "shps")
levels_dir = os.path.join(base_dir, "levels")

make_dir(raw_dir)
make_dir(shps_dir)
make_dir(levels_dir)


# -----------------------------------------------------------------------------
# metadata prep

area_table_path = os.path.join(base_dir, "Areas_and_Densities_Table_1.csv")
road_table_path = os.path.join(base_dir, "Blocks_and_Roads_Table_1.csv")

area_table_df =  pd.read_csv(
    area_table_path, header=None,
    quotechar='\"', na_values='', keep_default_na=False, encoding='utf-8')

road_table_df =  pd.read_csv(
    road_table_path, header=None,
    quotechar='\"', na_values='', keep_default_na=False, encoding='utf-8')


area_table_df = area_table_df.drop(area_table_df.columns[[79, 80]], axis=1)

area_table_header = area_table_df.iloc[[0,1]].copy(deep=True)
road_table_header = road_table_df.iloc[[0,1]].copy(deep=True)

area_table_df = area_table_df.drop([0,1], axis=0)
road_table_df = road_table_df.drop([0,1], axis=0)

area_table_df = area_table_df.loc[area_table_df[0].notnull()]
road_table_df = road_table_df.loc[road_table_df[0].notnull()]


rename_list = [
    ("Pematangtiantar", "Pematangsiantar"),
    ("Tebessa ", "Tebessa"),
    ("Tianjin,  Tianjin", "Tianjin, Tianjin")
]

for old, new in rename_list:
    area_table_df.loc[area_table_df[0] == old, 0] = new
    road_table_df.loc[road_table_df[0] == old, 0] = new

isnan = lambda x: x == "nan"

def clean_columns(df):
    columns = []
    for i in range(df.shape[1]):
        if i == 0:
            prev_top = "nan"
        top = str(df.iloc[0, i])
        bot = str(df.iloc[1, i])
        if not isnan(top) and isnan(bot):
            cname = top
        elif isnan(top) and not isnan(bot):
            cname = "{} {}".format(prev_top, bot)
        elif not isnan(top) and not isnan(bot):
            cname = "{} {}".format(top, bot)
        else:
            raise Exception("Error for column {} (top: {}, bot: {}, prev_top: {}".format(i, top, bot, prev_top))
        if not isnan(top):
            prev_top = top
        columns.append(cname)
    return columns


area_table_columns = clean_columns(area_table_header)
road_table_columns = clean_columns(road_table_header)

area_table_df.columns = area_table_columns
road_table_df.columns = road_table_columns

merge_field = "City Name"
tmp_cols = [merge_field] + [i for i in road_table_df.columns if i not in area_table_df.columns]

metadata_df = pd.merge(area_table_df, road_table_df[tmp_cols], on=merge_field)
metadata_df.to_csv(metadata_path, index=False, encoding='utf-8')


# -----------------------------------------------------------------------------
# download and extract

download = False
overwrite_download = False
extract = False

# url_base = "http://www.atlasofurbanexpansion.org/file-manager/userfiles/data_page/Phase%20I%20GIS"
url_base = "http://www.atlasofurbanexpansion.org/file-manager/userfiles/data_page/Phase I GIS"


valid_file_str = ["studyArea.", "urban_edge_t"]

def get_src_dst(city_name):
    file_name = city_name.replace(" ", "_") + ".zip"
    src = os.path.join(url_base, file_name)
    dst = os.path.join(raw_dir, file_name)
    return src, dst

# download
if download:
    for ix, row in metadata_df.iterrows():
        src, dst = get_src_dst(row[0])
        if src.endswith("Changzhi,_Hunan.zip"):
            src = src.replace("Changzhi,_Hunan.zip", "Changzhi_Shanxi.zip")
        if not os.path.isfile(dst) or overwrite_download:
            try:
                url_data = urllib2.urlopen(src)
            except Exception as e:
                print "Error: {}".format(src)
                print e
                continue
            with open(dst, 'wb') as f:
                f.write(url_data.read())

# extract
if extract:
    for ix, row in metadata_df.iterrows():
        _, dst = get_src_dst(row[0])
        with zipfile.ZipFile(dst) as zf:
            flist = [i for i in zf.namelist() if any(x in i for x in valid_file_str)]
            zf.extractall(shps_dir, flist)


# -----------------------------------------------------------------------------
# merge data




def reproject(geom, src_crs, dst_crs='epsg:4326'):
    project = partial(
        pyproj.transform,
        pyproj.Proj(init=src_crs),
        pyproj.Proj(init=dst_crs))
    return mapping(transform(project, shape(geom)))



# merge each level
levels = [
    # "studyArea",
    # "urban_edge_t3",
    "urban_edge_t2",
    "urban_edge_t1"]



for level in levels:
# level = levels[0]
    print "Processing level: {}".format(level)
    level_path = os.path.join(base_dir, "{}.geojson".format(level))
    feature_list = []
    for ix, row in metadata_df.iterrows():
    # for ix, row in metadata_df.loc[metadata_df["City Name"] == "Milan"].iterrows():
        city_name = row["City Name"].replace(" ", "_")
        fname = level + ".shp"
        if level == "studyArea":
            fname = "{}_{}".format(city_name, fname)
        shp_path = os.path.join(shps_dir, city_name, fname)
        shp = fiona.open(shp_path, "r")
        if len(shp) > 1:
            print "\tCombining {} features for {}".format(len(shp), city_name)
            geom = mapping(cascaded_union([shape(i["geometry"]) for i in shp]))
        else:
            geom = shp[0]["geometry"]
        # if "coordinates" in geom:
        #     geom = {
        #         "type": "Feature",
        #         "geometry": geom,
        #     }
        if shp.crs["init"] != 'epsg:4326':
            geom = reproject(geom, shp.crs["init"])
        if geom["type"] == "Polygon":
            geom = mapping(MultiPolygon([shape(geom)]))
        feature = {
            "id": ix,
            "type": "Feature",
            "geometry": geom,
            "properties": row.to_dict()
        }
        feature_list.append(feature)
        shp.close()


    # crs = {'init': 'epsg:4326'}
    # schema = {'geometry': 'MultiPolygon','properties': {i:'str' for i in metadata_df.columns}}

    # level_path = os.path.join(levels_dir, level + ".shp")
    # with fiona.open(level_path, 'w', driver='ESRI Shapefile', crs=crs, schema=schema) as c:
    #     c.writerecords(feature_list)

    # level_path = os.path.join(levels_dir, level + ".gpkg")
    # with fiona.open(level_path, 'w', driver='GPKG', crs=crs, schema=schema) as c:
    #     c.writerecords(feature_list)


    level_path = os.path.join(levels_dir, level + ".geojson")
    level_out = {
        "type": "FeatureCollection",
        "features": feature_list
    }
    level_file = open(level_path, "w")
    json.dump(level_out, level_file)
    level_file.close()
