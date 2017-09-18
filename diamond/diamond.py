# rasterize diamond deposit points and generate distance raster

import os
import fiona
# from shapely.geometry import Point
from distancerasters import rasterize, build_distance_array
from affine import Affine

# geo_dir = "pre_geo"
geo_dir = "geo"

src_path = r"/sciclone/aiddata10/REU/{0}/raw/diamond_201708/diamond/DIADATA.shp".format(geo_dir)

dst_dir = r"/sciclone/aiddata10/REU/{0}/data/rasters/diamond_201708".format(geo_dir)


binary_output_raster_path = os.path.join(dst_dir, "diamond_binary.tif")
distance_output_raster_path = os.path.join(dst_dir, "diamond_distance.tif")


pixel_size = 0.01

features = fiona.open(src_path)

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

diamond, _ = rasterize(features, output=binary_output_raster_path, pixel_size=pixel_size,affine=affine, shape=shape)


# ------------------------
# distance to diamond

def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(diamond, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)







