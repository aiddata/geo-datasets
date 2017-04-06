

from distancerasters import build_distance_array, rasterize, export_raster


# -----------------------------------------------------------------------------

from affine import Affine
import numpy as np

names = [
    'africa', 'americas', 'asia', 'europe', 'oceania-east', 'oceania-west'
]

pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)

shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

roads = np.zeros(shape, dtype='byte')

for n in names:
    path = "{0}/{1}".format("/sciclone/aiddata10/REU/raw/groads",
                            "groads-v1-{0}-shp/gROADS-v1-{0}.shp".format(n))
    rv_array, _ = rasterize(path, affine=affine, shape=shape)
    roads = roads | rv_array


roads_output_raster_path = "/sciclone/aiddata10/REU/data/rasters/external/global/distance_to/groads/binary/groads_binary.tif"

export_raster(roads, affine, roads_output_raster_path)


# -----------------------------------------------------------------------------

# import rasterio
# roads_src = rasterio.open(roads_output_raster_path)
# roads = roads_src.read()[0]
# affine = roads_src.affine

distance_output_raster_path = "/sciclone/aiddata10/REU/data/rasters/external/global/distance_to/groads/groads_distance.tif"


def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(roads, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


