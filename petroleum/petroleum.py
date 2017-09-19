# rasterize polygons as binary and generate distance raster

import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array

# geo_dir = "pre_geo"
geo_dir = "geo"

src_path = r"/sciclone/aiddata10/REU/{0}/raw/petroleum_v12/PETRODATA V1.2/Petrodata_Onshore_V1.2.shp".format(geo_dir)
dst_dir = r"/sciclone/aiddata10/REU/{0}/data/rasters/petroleum_v12/onshore".format(geo_dir)


binary_output_raster_path = os.path.join(dst_dir, "petroleum_binary.tif")
distance_output_raster_path = os.path.join(dst_dir, "petroleum_distance.tif")



features = fiona.open(src_path)


pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)



print "Rasterizing"

petroleum, _ = rasterize(vectors=features, output=binary_output_raster_path,
                         pixel_size=pixel_size, affine=affine, shape=shape)


# --------------------------------------
# distance to gem

print "Generating distance raster"

def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(petroleum, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


