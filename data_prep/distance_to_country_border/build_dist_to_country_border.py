

from distancerasters import build_distance_array, rasterize, export_raster


# -----------------------------------------------------------------------------

from affine import Affine
import numpy as np


borders_path = "/sciclone/aiddata10/REU/raw/gadm28_country_borders/gadm28_adm0_lines.shp"


pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)


shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

borders, _ = rasterize(path=borders_path, affine=affine, shape=shape)



borders_output_raster_path = "/sciclone/aiddata10/REU/data/rasters/external/global/distance_to/gadm28_borders/binary/gadm28_borders_binary.tif"

export_raster(borders, affine, borders_output_raster_path)


# -----------------------------------------------------------------------------

# import rasterio
# borders_src = rasterio.open(borders_output_raster_path)
# borders = borders_src.read()[0]
# affine = borders_src.affine

distance_output_raster_path = "/sciclone/aiddata10/REU/data/rasters/external/global/distance_to/gadm28_borders/gadm28_borders_distance.tif"


def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(borders, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


