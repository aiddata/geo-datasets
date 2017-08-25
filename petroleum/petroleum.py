

# polygons
import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array

# indir = r"/Users/miranda/Documents/AidData/sciclone/datasets/natural_resource/petroleum/PETRODATA V1.2/Petrodata_Onshore_V1.2.shp"
indir = r"/sciclone/home10/zlv/datasets/data_process/natural_resource/petroleum/PETRODATA V1.2/Petrodata_Onshore_V1.2.shp"
outfile = r"/sciclone/data20/zlv/data_process/natural_resource/petroleum/petroleum.tif"
#outfile = r"/Users/miranda/Documents/AidData/sciclone/datasets/natural_resource/petroleum/petroleum.tif"


pixel_size = 0.01
# files = [os.path.join(indir, f) for f in os.listdir(indir) if f.endswith(".shp") and os.path.isfile(os.path.join(indir, f))]

features = fiona.open(indir)

bound = features.bounds

# outf = os.path.join(outdir, (os.path.splitext(os.path.basename(file))[0] + ".tif"))

petroleum, aff = rasterize(vectors=features, pixel_size=pixel_size, bounds=bound, output=outfile)


# --------------------------------------
# distance to gem

distance_output_raster_path = "/sciclone/data20/zlv/data_process/natural_resource/petroleum/petroleum_distance.tif"


def raster_conditional(rarray):
    return (rarray == 1)

dist = build_distance_array(petroleum, affine=aff,
                            output=distance_output_raster_path,
                            conditional=raster_conditional)


