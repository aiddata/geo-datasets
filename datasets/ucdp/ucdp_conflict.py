"""
binary of area that experienced conflict

UCDP GED Polygons
http://ucdp.uu.se/downloads/

Download to raw directory (include version in path)

Update raw directory source and output in data directory
(with corresponding version in path)
"""


import os
import errno
import math
import fiona
from affine import Affine

from distancerasters import rasterize, export_raster


# -----------------------------------------------------------------------------


input_path = "/sciclone/aiddata10/REU/geo/raw/ucdp/ged_polygons_v-1-1/ucdp-ged-poly-v-1-1-shape/UCDPGEDpolyyear.shp"

output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/ucdp/ged_polygons_v-1-1"

pixel_size = 0.01


# -----------------------------------------------------------------------------


def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


make_dir(output_dir)

features_input = fiona.open(input_path)


minx, miny, maxx, maxy = features_input.bounds

minx = math.floor(minx)
miny = math.floor(miny)
maxx = math.ceil(maxx)
maxy = math.ceil(maxy)


out_shape = (int((maxy - miny) / pixel_size), int((maxx - minx) / pixel_size))

affine = Affine(pixel_size, 0, minx,
                0, -pixel_size, maxy)



for year in range(1989, 2011):
    features_filtered = [f for f in features_input if (f["properties"]["year"] == year)]

    print "selected {0} features for year: {1}".format(len(features_filtered), year)

    if len(features_filtered) == 0:
        print "\tno feature selected for year {1}".format(year)
        continue

    cat_raster, _ = rasterize(features_filtered, affine=affine, shape=out_shape)

    output_file = "ucdp_conflict_{0}.tif".format(year)
    output_path = os.path.join(output_dir, output_file)

    export_raster(cat_raster, affine=affine, path=output_path, out_dtype='uint8', nodata=None)

