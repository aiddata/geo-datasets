



import os
import fiona
import rasterio
import numpy as np
from affine import Affine
from rasterio import features
import time

from distancerasters import rasterize, export_raster


# -----------------------------------------------------------------------------

# shp_path = r"/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/ucdp-ged-poly-v-1-1-shape/UCDPGEDpolyyear.shp"

shp_path = "/sciclone/home10/zlv/datasets/ucdp/ucdp-ged-poly-v-1-1-shape/UCDPGEDpolyyear.shp"

pixel_size = 0.01

field_name = "type_of_vi"
field_values = {1: "State-based", 2: "Non-state", 3: "One-sided"}

final_output = "/sciclone/data10/zlv/ucdp/output"
# final_output = "/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/output"

# -----------------------------------------------------------------------------

features_input = fiona.open(shp_path)

# -----------------------------------------------------------------------------

try:
    pixel_size = float(pixel_size)
except:
    raise Exception("Invalid pixel size (could not be converted to float)")

out_shape = (int(180 / pixel_size), int(360 / pixel_size))

affine = Affine(pixel_size, 0, -180,
                0, -pixel_size, 90)


t1 = time.time()


for cat in field_values.keys():

    for year in range(1989, 2011):
        features_filtered = [f for f in features_input
                             if (f["properties"][field_name] == cat) and (f["properties"]["year"] == year)]

        # build intermediary rasters for individual categories/years

        cat_layers = list()

        print "selected {0} features for field: {1}, and year: {2}".format(
            len(features_filtered), cat, year)

        if len(features_filtered) == 0:
            print "\tno feature selected for violence type {0} and year {1}".format(cat, year)
            pass


        else:

            for feature in features_filtered:

                cat_raster, _ = rasterize(feature, affine=affine, shape=out_shape)

                cat_layers.append(cat_raster)

        # cat_raster, _ = rasterize(features_filtered, affine=affine, shape=out_shape)
        # cat_layers.append(cat_raster)

        # output_file = "ucdp_" + field_values[cat] + "_" + str(year) + ".tif"
        # output_path = os.path.join(final_output, output_file)
        # export_raster(cat_raster, affine=affine, path=output_path)


        output_raster = np.zeros(shape=(out_shape[0], out_shape[1]))

        for index in range(len(cat_layers)):
            print "merging layer {0}".format(index)
            print cat_layers[index].sum()
            print cat_layers[index].dtype
            output_raster += cat_layers[index]
            # output_raster = np.where(output_raster > (index + 1), 11, output_raster)

        print "sum of output raster, ", output_raster.sum()
        print output_raster.dtype

        output_file = "ucdp_" + field_values[cat] + "_" + str(year) + ".tif"
        output_path = os.path.join(final_output, output_file)

        # with rasterio.open(output_path, "w", driver='GTiff', width=out_shape[1], height=out_shape[0], count=1, dtype="float64")
        export_raster(output_raster, affine=affine, path=output_path, out_dtype='float64', nodata=None)

t2 = time.time()

sum_time = t2 - t1

print sum_time