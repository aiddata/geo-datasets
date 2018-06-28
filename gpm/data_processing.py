
import os
import shutil

inpath = r"/sciclone/aiddata10/REU/pre_geo/GPM/raw/gis"
outpath = r"/sciclone/aiddata10/REU/pre_geo/GPM/processed"

infiles = [f for f in os.listdir(inpath) if os.path.isfile(os.path.join(inpath, f)) and f.endswith("tif")]


def rename_data(inf):
	
	date = f.split(".")[4].split("-")[0]
	year = date[:4]
	month = str(date[4:6])
	name = "gpm_precipitation_%s_%s.tif"%(year,month)
	return name


for f in infiles:
	newf = os.path.join(outpath,rename_data(f))
	oldf = os.path.join(inpath,f)
	shutil.copy2(oldf,newf)
