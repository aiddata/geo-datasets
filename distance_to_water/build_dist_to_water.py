

from distancerasters import build_distance_array, rasterize, export_raster


# -----------------------------------------------------------------------------

from affine import Affine
import numpy as np


shorelines_path = "/sciclone/aiddata10/REU/geo/raw/gshhg/gshhg-shp-2.3.7/GSHHS_shp/f/GSHHS_f_L1.shp"
lakes_path = "/sciclone/aiddata10/REU/geo/raw/natural-earth-vector/d4533efe3715c55b51f49bc2bde9694bff2bf7b1/10m_physical/ne_10m_lakes.shp"
rivers_path = "/sciclone/aiddata10/REU/geo/raw/natural-earth-vector/d4533efe3715c55b51f49bc2bde9694bff2bf7b1/10m_physical/ne_10m_rivers_lake_centerlines.shp"


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

lakes, _ = rasterize(lakes_path, affine=affine, shape=shape)
rivers, _ = rasterize(rivers_path, affine=affine, shape=shape)


water = shorelines + lakes + rivers


water_output_raster_path = "/sciclone/aiddata10/REU/geo/data/rasters/distance_to/water/binary/water_binary.tif"

export_raster(water, affine, water_output_raster_path)


# -----------------------------------------------------------------------------

# import rasterio
# water_src = rasterio.open(water_output_raster_path)
# water = water_src.read()[0]
# affine = water_src.affine

distance_output_raster_path = "/sciclone/aiddata10/REU/geo/data/rasters/distance_to/water/water_distance.tif"


def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(water, affine=affine,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


