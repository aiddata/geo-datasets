import os
import pandas as pd
import matplotlib.pyplot as plt
# import seaborn as sns
import geopandas as gpd
from shapely.geometry import Point
import numpy as np

from affine import Affine
from distancerasters import rasterize, export_raster

# cd Desktop first
file = "Merged_Stacked_Datasets_v3.dta"

# r"/Users/miranda/Documents/AidData/datasets/AFB/rasterization/Merged Stacked Datasets_v3.dta"
# r"/sciclone/home10/zlv/datasets/data_process/afb/afb_data_merged.dta"
dirc = os.path.dirname(file)

# for rd in range(1, 7):
#     folder = "round_" + str(rd)
#     fpath = os.path.join(dirc, folder)
#     if not os.path.isdir(fpath):
#         os.mkdir(fpath)


df = pd.read_stata(file)

questions = ["trust_pres"]#, "trust_police", "trust_court", "trust_electcom",
             #"trust_party", "trust_oppart"]


# ------ rasterization setting ---------

pixel_size = 0.01

out_shape = (int(180 / pixel_size), int(360 / pixel_size))

affine = Affine(pixel_size, 0, -180,
                0, -pixel_size, 90)


rd = 1
# for rd in range(1, 2):

# set path
folder = "round_" + str(rd)
fpath = os.path.join(dirc, folder)

dta = df.loc[df['round'] == rd].copy(deep=True)
dta.loc['category'] = None



# get A
dta.loc[((dta["location_class"] == 2) | (dta["location_class"] == 3) | ((dta["location_class"] == 1) & (dta["location_type_code"].isin(["ADM3", "ADM4", "ADM4H", "ADM5"])))), 'category'] = 'A'
dta_asub = dta.loc[dta['category'] == 'A']

respno_a = list(dta_asub["respno"])


# get B
dta.loc[(~dta["respno"].isin(respno_a)), 'category'] = 'B'
dta_bsub = dta.loc[dta['category'] == 'B']


dta_sum = pd.concat([dta_asub, dta_bsub])
#print dta_sum.shape

dta_sum['geometry'] = dta_sum.apply(lambda z: Point(z['longitude'], z['latitude']), axis=1)
gdf = gpd.GeoDataFrame(dta_sum)

question = questions[0]
# for question in questions:

# if not gdf[question].isnull().all():

"""

# categorical plot
plot = sns.countplot(x=question, hue="category", data=gdf)

fig = plot.get_figure()
name = "category_" + question + ".png"
fig.savefig(os.path.join(fpath, name))
plt.clf()


# scatterplot of coordinates

plt.clf()
num = "N = " + str(len(dta_asub))
plt.plot(dta_asub["longitude"], dta_asub["latitude"], 'r.', markersize=2)
name = "Category_A: " + question + "_" + str(rd)
plt.title(name)
name = "r_" + str(rd) + "_scattorplot_a_" + question
plt.savefig(os.path.join(os.path.join(fpath, name)))

plt.clf()
num = "N = " + str(len(dta_bsub))
plt.plot(dta_bsub["longitude"], dta_bsub["latitude"], 'X', markersize=2)
name = "Category_B: " + question + "_" + str(rd)
plt.title(name)
name = "r_" + str(rd) + "_scattorplot_b_" + question
plt.savefig(os.path.join(fpath, name))
plt.clf()
"""

# rasterize

rasterdf = gdf.loc[gdf["category"] == "A"]

output1 = "delete.csv"
rasterdf.to_csv(output1, encoding='utf-8', sep=',')

name = "round_{0}_{1}.tif".format(rd, question)
output = os.path.join(fpath, name)

print question

cat_raster, _ = rasterize(rasterdf, affine=affine, shape=out_shape, attribute=question, nodata=0, fill=-999)

output = "delete.tif"
export_raster(cat_raster, affine=affine, path=output)


"""
raster_layer = list()

for value in values:

    rasterdata = rasterdf[rasterdf[question] == value]

    print rasterdata.shape
    #cat_raster, _ = rasterize(rasterdata, affine=affine, shape=out_shape, nodata=-99)
    #raster_layer.append(cat_raster)

#output_raster = np.zeros(shape=(out_shape[0], out_shape[1]))

#for index in range(len(raster_layer)):

    #output_raster = output_raster + raster_layer[index] * (index + 1)

#export_raster(output_raster, affine=affine, path=output)
"""

