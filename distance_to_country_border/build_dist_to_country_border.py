
import distancerasters as dr
# from distancerasters import build_distance_array, rasterize, export_raster


# -----------------------------------------------------------------------------

from affine import Affine
import numpy as np


borders_path = "/sciclone/aiddata10/REU/geo/raw/geoBoundaries_country_borders/geoBoundariesCGAZ_ADM0.geojson"

pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)


shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

borders, _ = dr.rasterize(borders_path, affine=affine, shape=shape)



borders_output_raster_path = "/sciclone/aiddata10/REU/geo/raw/geoBoundaries_country_borders/binary/geoboundaries_borders_binary.tif"

dr.export_raster(borders, affine, borders_output_raster_path)


# -----------------------------------------------------------------------------

# import rasterio
# borders_src = rasterio.open(borders_output_raster_path)
# borders = borders_src.read()[0]
# affine = borders_src.affine

distance_output_raster_path = "/sciclone/aiddata10/REU/geo/raw/geoBoundaries_country_borders/geoboundaries_borders_distance.tif"


def raster_conditional(rarray):
    return (rarray == 1)

dist = dr.DistanceRaster(borders, affine=affine,
                            output_path=distance_output_raster_path,
                            conditional=raster_conditional)


