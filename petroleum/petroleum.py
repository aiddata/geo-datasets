

# polygons
import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array

indir = r"/sciclone/aiddata10/REU/pre_geo/raw/prio/petroleum/PETRODATA V1.2/Petrodata_Onshore_V1.2.shp"
outdir = r"/sciclone/aiddata10/REU/pre_geo/data"


if not os.path.exists(os.path.join(outdir,"petroleum")):
    os.makedirs(os.path.join(outdir,"petroleum"))

outfile = os.path.join(outdir, "petroleum", "petroleum.tif")
distance_output_raster_path = os.path.join(outdir, "petroleum", "petroleum_distance.tif")


pixel_size = 0.01

features = fiona.open(indir)

bound = features.bounds

petroleum, aff = rasterize(vectors=features, pixel_size=pixel_size, bounds=bound, output=outfile)


# --------------------------------------
# distance to gem

def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(petroleum, affine=aff,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


