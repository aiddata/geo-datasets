

# polygons
import os
import fiona
from distancerasters import rasterize, export_raster, build_distance_array
from affine import Affine
import numpy as np




#indir = r"/Users/miranda/Documents/AidData/sciclone/datasets/natural_resource/drug/drugdata/DRUGDATA ArcGIS files"
indir = r"/sciclone/home10/zlv/datasets/data_process/natural_resource/drug/DRUGDATA ArcGIS files"
#out_cat = r"/Users/miranda/Documents/AidData/sciclone/datasets/natural_resource/drug/test_delete.tif"
out_cat = r"/sciclone/data20/zlv/data_process/natural_resource/drug/drug_prio.tif"

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


export_raster(ini_image, affine=affine, nodata=0, path=out_cat)







