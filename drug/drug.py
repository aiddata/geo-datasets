

# polygons
import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array
from affine import Affine
import numpy as np



indir = r"/sciclone/aiddata10/REU/pre_geo/raw/prio/drug/drugdata"
outdir = r"/sciclone/aiddata10/REU/pre_geo/data"


if not os.path.exists(os.path.join(outdir,"drug")):

    os.makedirs(os.path.join(outdir,"drug"))
    outfile = os.path.join(outdir, "drug", "drug.tif")

else:

    outfile = os.path.join(outdir, "drug", "drug.tif")


pixel_size = 0.01

xmin = -180
xmax = 180
ymin = -90
ymax = 90

shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

affine = Affine(pixel_size, 0, xmin,
                0, -pixel_size, ymax)



files = [os.path.join(indir, f) for f in os.listdir(indir) if f.endswith(".shp") and os.path.isfile(os.path.join(indir, f))]

ini_image = np.zeros(shape=(shape[0], shape[1]))


if len(files) < 4:

    for i in range(len(files)):

        print "working on, ", files[i]

        features = fiona.open(files[i])

        rst, _ = rasterize(vectors=features, pixel_size=pixel_size, affine=affine, shape=shape)

        sum_image = rst * (i+1) + ini_image
        sum_image = np.where(sum_image > (i+1), 4, sum_image)
        ini_image = sum_image

else:

    raise("More than 3 drug layers")


export_raster(ini_image, affine=affine, nodata=0, path=outfile)







