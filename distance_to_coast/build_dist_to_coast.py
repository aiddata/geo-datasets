

from distancerasters import build_distance_array, rasterize, export_raster


# -----------------------------------------------------------------------------

from affine import Affine
import numpy as np


shorelines_path = "/sciclone/aiddata10/REU/geo/raw/gshhg/gshhg-shp-2.3.6/GSHHS_shp/f/GSHHS_f_L1.shp"


pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)


shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

shorelines, _ = rasterize(shorelines_path, affine=affine, shape=shape)
shorelines = np.logical_not(shorelines).astype(int)


water_output_raster_path = "/sciclone/aiddata10/REU/geo/data/rasters/distance_to/coast_236/binary/coast_binary.tif"

export_raster(shorelines, affine, water_output_raster_path)


# -----------------------------------------------------------------------------

# import rasterio
# water_src = rasterio.open(water_output_raster_path)
# water = water_src.read()[0]
# affine = water_src.affine

distance_output_raster_path = "/sciclone/aiddata10/REU/geo/data/rasters/distance_to/coast_236/coast_distance.tif"


def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(shorelines, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


