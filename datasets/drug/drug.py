

# polygons
import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array
from affine import Affine
import numpy as np

# geo_dir = "pre_geo"
geo_dir = "geo"


src_dir = r"/sciclone/aiddata10/REU/{0}/raw/drugdata_201708/drugdata/DRUGDATA ArcGIS files".format(geo_dir)
src_names = [r"CANNABIS", r"COCA BUSH", r"OPIUM POPPY"]
src_files = [os.path.join(src_dir, name + ".shp",) for name in src_names]

dst_dir = r"/sciclone/aiddata10/REU/{0}/data/rasters/drugdata_201708".format(geo_dir)


categorical_output_raster_path = os.path.join(dst_dir, "drug_categorical.tif")
distance_output_raster_path = os.path.join(dst_dir, "drug_distance.tif")


pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)

print "Rasterizing"


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


export_raster(output, affine=affine, nodata=255, path=categorical_output_raster_path)



# ------------------------
# distance raster

print "Generating distance raster"

def raster_conditional(rarray):
    return (rarray > 0)

dist = build_distance_array(output, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)







