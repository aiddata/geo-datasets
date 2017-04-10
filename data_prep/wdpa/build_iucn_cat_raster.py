"""
build iucn categorical raster from wdpa data
"""

import fiona
import rasterio
import numpy as np
from affine import Affine
from rasterio import features

from distancerasters import rasterize, export_raster


# -----------------------------------------------------------------------------


shp_path = "/sciclone/aiddata10/REU/raw/wdpa/WDPA_Apr2017_Public/WDPA_Apr2017_Public.gdb"

pixel_size = 0.01

field_name = "IUCN_CAT"
field_values = [
    "Ia", "Ib", "II", "III", "IV", "V", "VI",
    "Not Applicable", "Not Assigned", "Not Reported"
]

final_output = "/sciclone/aiddata10/REU/data/rasters/external/global/wdpa/iucn_cat_201704/wdpa_iucn_cat_201704.tif"


# -----------------------------------------------------------------------------


# load data and init key variables

layers = fiona.listlayers(shp_path)
poly_layer = [i for i in layers if "poly" in i]

if len(poly_layer) > 1:
    raise Exception("multiple potential polygon layers found")
elif len(poly_layer) == 0:
    raise Exception("no potential polygon layer found")

features_input = fiona.open(shp_path, layer=poly_layer[0])


###

# final_output = "/home/userz/Desktop/potential_data/wdpa/iucn_cat_201704/wdpa_iucn_cat_201704.tif"

# shp_path = "/home/userz/Desktop/potential_data/wdpa/WDPA_Apr2017_poly_subset.shp"

# features_input = fiona.open(shp_path)

###


try:
    pixel_size = float(pixel_size)
except:
    raise Exception("Invalid pixel size (could not be converted to float)")

out_shape = (int(180 / pixel_size), int(360 / pixel_size))

affine = Affine(pixel_size, 0, -180,
                0, -pixel_size, 90)


# build intermediary rasters for individual categories

cat_layers = list()

for cat in field_values:
    features_filtered = [f for f in features_input
                         if f["properties"][field_name] == cat]

    print "selected {0} features for field: {1}".format(
        len(features_filtered), cat)

    if len(features_filtered) == 0:
        print "\tno feature selected for year {0}".format(cat)
        pass

    cat_raster, _ = rasterize(features_filtered, affine=affine, shape=out_shape)
    cat_layers.append(cat_raster)


# merge category rasters and output

output_raster = np.zeros(shape=(out_shape[0], out_shape[1]))

for index in range(len(cat_layers)):
    print "merging layer {0}".format(index)
    output_raster = output_raster + cat_layers[index] * (index + 1)
    output_raster = np.where(output_raster > (index + 1), 11, output_raster)


export_raster(output_raster, affine=affine, path=final_output)


