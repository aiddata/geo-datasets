

# points

import os
import fiona
from shapely.geometry import Point
from distancerasters import rasterize, build_distance_array
from affine import Affine


indir = r"/sciclone/aiddata10/REU/pre_geo/raw/prio/gem/gemdata/GEMDATA.shp"

outdir = r"/sciclone/aiddata10/REU/pre_geo/data"


if not os.path.exists(os.path.join(outdir,"gemdata")):
    os.makedirs(os.path.join(outdir,"gemdata"))

outfile = os.path.join(outdir, "gemdata", "gemstone.tif")
distance_output_raster_path = os.path.join(outdir, "gemdata", "gemstone_distance.tif")



pixel_size = 0.01

features = fiona.open(indir)


(xmin, ymin, xmax, ymax) = features.bounds

shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)

gem, _ = rasterize(features, output=outfile, pixel_size=pixel_size,affine=affine, shape=shape)


# --------------------------------------
# distance to gem

def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(gem, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)





