# rasterize polygons as binary and generate distance raster

import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array


src_path = r"/sciclone/aiddata10/REU/pre_geo/raw/prio/petroleum/PETRODATA V1.2/Petrodata_Onshore_V1.2.shp"
dst_dir = r"/sciclone/aiddata10/REU/pre_geo/data/rasters/onshore_petroleum_v12"


binary_output_raster_path = os.path.join(dst_dir, "petroleum_binary.tif")
distance_output_raster_path = os.path.join(dst_dir, "petroleum_distance.tif")


pixel_size = 0.01

features = fiona.open(src_path)

bound = features.bounds

petroleum, aff = rasterize(vectors=features, pixel_size=pixel_size, bounds=bound, output=distance_output_raster_path)


# --------------------------------------
# distance to gem

def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(petroleum, affine=aff,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


