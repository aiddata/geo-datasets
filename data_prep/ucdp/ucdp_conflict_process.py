

# binary conflict output

import os
import fiona
import rasterio
import numpy as np
from affine import Affine
from rasterio import features
import time
import math

from distancerasters import rasterize, export_raster


# -----------------------------------------------------------------------------

shp_path = r"/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/ucdp-ged-poly-v-1-1-shape/UCDPGEDpolyyear.shp"

# shp_path = "/sciclone/home10/zlv/datasets/ucdp/ucdp-ged-poly-v-1-1-shape/UCDPGEDpolyyear.shp"

pixel_size = 0.01

# field_name = "type_of_vi"
# field_values = {1: "State-based", 2: "Non-state", 3: "One-sided"}

# final_output = "/sciclone/data20/zlv/ucdp/output_africa"
final_output = "/Users/miranda/Documents/AidData/sciclone/datasets/UCDP_GED_conflict/output_africa"

# -----------------------------------------------------------------------------

features_input = fiona.open(shp_path)

# -----------------------------------------------------------------------------

try:
    pixel_size = float(pixel_size)
except:
    raise Exception("Invalid pixel size (could not be converted to float)")

minx, miny, maxx, maxy = features_input.bounds

minx = math.floor(minx)
miny = math.floor(miny)
maxx = math.ceil(maxx)
maxy = math.ceil(maxy)


out_shape = (int((maxy - miny) / pixel_size), int((maxx - minx) / pixel_size))

affine = Affine(pixel_size, 0, minx,
                0, -pixel_size, maxy)


t1 = time.time()


for year in range(1989, 2011):
    features_filtered = [f for f in features_input
                        if (f["properties"]["year"] == year)]

    print "selected {0} features for year: {1}".format(
            len(features_filtered), year)

    if len(features_filtered) == 0:
        print "\tno feature selected for year {1}".format(year)
        pass


    else:

        cat_raster, _ = rasterize(features_filtered, affine=affine, shape=out_shape)


    output_file = "ucdp_conflict_" + str(year) + ".tif"
    output_path = os.path.join(final_output, output_file)

    export_raster(cat_raster, affine=affine, path=output_path, out_dtype='float64', nodata=None)

t2 = time.time()

sum_time = t2 - t1

print sum_time