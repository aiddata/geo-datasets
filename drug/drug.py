

# polygons
import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array
from affine import Affine
import numpy as np


src_dir = r"/sciclone/aiddata10/REU/pre_geo/raw/drug/drugdata/DRUGDATA ArcGIS files"
src_names = [r"CANNABIS", r"COCA BUSH", r"OPIUM POPPY"]
src_files = [os.path.join(src_dir, name + ".shp",) for name in src_names]

dst_dir = r"/sciclone/aiddata10/REU/pre_geo/data/rasters/drug_201708"


output_raster_path = os.path.join(dst_dir, "drug.tif")


pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)


# initialize output array
output = np.zeros(shape=(shape[0], shape[1]))

mixed_val = 4

i = 0
for f in src_files:
    i += 1
    print "Processing: ", f

    features = fiona.open(f)

    rv_array, _ = rasterize(vectors=features, pixel_size=pixel_size,
                            affine=affine, shape=shape)

    output += rv_array * i

    # any cell with value > i must have multiple features
    output = np.where(output > i, mixed_val, output)


export_raster(output, affine=affine, nodata=255, path=output_raster_path)







