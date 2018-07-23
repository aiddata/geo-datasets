
import os
import shutil
import numpy as np
from osgeo import gdal
from affine import Affine
import distancerasters

inpath = r"/sciclone/aiddata10/REU/pre_geo/GPM/raw/gis"
outpath = r"/sciclone/aiddata10/REU/pre_geo/GPM/processed"

infiles = [f for f in os.listdir(inpath) if os.path.isfile(os.path.join(inpath, f)) and f.endswith("tif")]


def rename_data(inf):

	date = f.split(".")[4].split("-")[0]
	year = date[:4]
	month = str(date[4:6])
	name = "gpm_precipitation_%s_%s.tif"%(year,month)
	return name



pixel_size = 0.1
affine = Affine(pixel_size, 0, -180,
                    0, -pixel_size, 90)


for f in infiles:
	newf = os.path.join(outpath,rename_data(f))
	oldf = os.path.join(inpath,f)
	#shutil.copy2(oldf,newf)
	ds = gdal.Open(oldf)
	dta_array = np.array(ds.GetRasterBand(1).ReadAsArray())
	distancerasters.export_raster(dta_array,affine,newf, nodata=-9999)


