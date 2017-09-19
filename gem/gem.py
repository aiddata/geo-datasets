# rasterize gemdata deposit points and generate distance raster

import os
import fiona
from shapely.geometry import Point
from distancerasters import rasterize, build_distance_array
from affine import Affine

# geo_dir = "pre_geo"
geo_dir = "geo"

src_path = r"/sciclone/aiddata10/REU/{0}/raw/gemdata_201708/gemdata/GEMDATA.shp".format(geo_dir)

dst_dir = r"/sciclone/aiddata10/REU/{0}/data/rasters/gemdata_201708".format(geo_dir)


binary_output_raster_path = os.path.join(dst_dir, "gemstone_binary.tif")
distance_output_raster_path = os.path.join(dst_dir, "gemstone_distance.tif")



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

gem, _ = rasterize(features, output=binary_output_raster_path, pixel_size=pixel_size,affine=affine, shape=shape)


# --------------------------------------
# distance to gem

print "Generating distance raster"

def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(gem, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)





