


import pandas as pd
import geopandas as gpd

from shapely.geometry import Point
from affine import Affine
from distancerasters import rasterize



# -------------------------------------
# define spatial bounds

pixel_size = 0.01

out_shape = (int(180 / pixel_size), int(360 / pixel_size))

affine = Affine(pixel_size, 0, -180,
                0, -pixel_size, 90)


# -------------------------------------
# load raw data

file = "Merged_Stacked_Datasets_v3.dta"

df = pd.read_stata(file)


# -------------------------------------
# subset definitions

question = "trust_pres"

rd = 1

nan_val = 255


# -------------------------------------
# define precision categories

is_precise = ((df["location_class"] == 2) | (df["location_class"] == 3) | ((df["location_class"] == 1) & (df["location_type_code"].isin(["ADM3", "ADM4", "ADM4H", "ADM5"]))))

df['category'] = None

df.loc[is_precise, 'category'] = 'A'
df.loc[~is_precise, 'category'] = 'B'


# -------------------------------------
# assign point geom

df['geometry'] = df.apply(lambda z: Point(z['longitude'], z['latitude']), axis=1)


# -------------------------------------
# remove nan vals

df.loc[df[question].isnull(), question] = nan_val


# -------------------------------------

subset = ((df['round'] == rd) & (df["category"] == "A"))

dta = df.loc[subset].copy(deep=True)


gdf = gpd.GeoDataFrame(dta)



csv_output = "delete.csv"
raster_output = "delete.tif"

gdf.to_csv(csv_output, encoding='utf-8', sep=',')


cat_raster, _ = rasterize(gdf, affine=affine, shape=out_shape, attribute=question, nodata=255, fill=255, output=raster_output)


