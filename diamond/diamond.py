

# points

import os
import fiona
# from shapely.geometry import Point
from distancerasters import rasterize, build_distance_array
from affine import Affine


indir = r"/sciclone/aiddata10/REU/pre_geo/raw/prio/diamond/GIS filene/DIADATA.shp"

outdir = r"/sciclone/aiddata10/REU/pre_geo/data"

if not os.path.exists(os.path.join(outdir,"diamond")):
    os.makedirs("diamond")

outfile = os.path.join(outdir, "diamond", "diamond.tif")
distance_output_raster_path = os.path.join(outdir, "diamond", "diamond_distance.tif")


pixel_size = 0.01

features = fiona.open(indir)

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

diamond, _ = rasterize(features, output=outfile, pixel_size=pixel_size,affine=affine, shape=shape)


# ------------------------
# distance to diamond

def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(diamond, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)







