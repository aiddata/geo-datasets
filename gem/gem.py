

# points

import os
import fiona
from shapely.geometry import Point
from distancerasters import rasterize, build_distance_array
from affine import Affine


indir = r"/sciclone/home10/zlv/datasets/data_process/natural_resource/gem/gemdata/GEMDATA.shp"

outdir = r"/sciclone/data20/zlv/data_process/natural_resource/gem/gem_prio.tif"


pixel_size = 0.01
#files = [os.path.join(indir, f) for f in os.listdir(indir) if f.endswith(".shp") and os.path.isfile(os.path.join(indir, f))]


features = fiona.open(indir)

# probably no need to buffer, since the resolution is 0.01, and there is no meaning for buffer distance 0.01

"""
shapes = [Point(feat['geometry']['coordinates'][0], feat['geometry']['coordinates'][1]).buffer(0.1) for feat in features]

minx = [shape.bounds[0] for shape in shapes]
miny = [shape.bounds[1] for shape in shapes]
maxx = [shape.bounds[2] for shape in shapes]
maxy = [shape.bounds[3] for shape in shapes]


bound = (min(minx), min(miny), max(maxx), max(maxy))
"""

(xmin, ymin, xmax, ymax) = features.bounds

shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)

gem, _ = rasterize(features, output=outdir, pixel_size=pixel_size,affine=affine, shape=shape)


# --------------------------------------
# distance to gem

distance_output_raster_path = "/sciclone/data20/zlv/data_process/natural_resource/gem/gem_distance.tif"


def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(gem, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)





