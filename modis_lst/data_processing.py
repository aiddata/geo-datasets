
"""
Author: Miranda Lv
Date: 06/21/2018
This script is used to:
    - read hdf data files and extract day/night land surface temperature.
    - the raw data was in 3600*7200 matrix, and there is no re-projection.
"""


from datetime import datetime
from pyhdf.SD import SD, SDC
import distancerasters
from affine import Affine
import os


inpath = r"/sciclone/aiddata10/REU/pre_geo/modis_temp/rawdata"
#file_name = r"/Users/miranda/Documents/AidData/projects/datasets/MODIS_temp/MOD11C3.A2009032.006.2016007161930.hdf"
outdir = r"/sciclone/aiddata10/REU/pre_geo/modis_temp/temp"

scale_factor = 0.02
pixel_size = 0.05
affine = Affine(pixel_size,0,-180,
                0,-pixel_size,90)

def get_time(datestring):

    datatime = datetime.strptime(datestring, "%Y%j")

    year = "%d"%datatime.year
    mon = "%02d"%datatime.month

    newtime = str(year) + "_" + str(mon)

    return newtime


def get_hdf(infile, outdir):

    # get data time stamp
    datestring = infile.split(".")[1][1:]
    newtime = get_time(datestring)

    # get new data export name
    dayout = os.path.join(outdir, "modis_lst_day_cmg_" + newtime + ".tif")
    nightout = os.path.join(outdir, "modis_lst_night_cmg_" + newtime + ".tif")

    # read hdf data files
    file = SD(infile, SDC.READ)
    #sds = file.datasets()

    day_img = file.select('LST_Day_CMG')
    day_dta = day_img.get() 
    day_dta = day_dta * scale_factor
    distancerasters.export_raster(day_dta,affine,dayout)

    night_img = file.select('LST_Night_CMG')
    night_dta = night_img.get() 
    night_dta = night_dta * scale_factor
    distancerasters.export_raster(night_dta, affine, nightout)


infiles = [f for f in os.listdir(inpath) if os.path.isfile(os.path.join(inpath, f)) and f.endswith("hdf")]


for f in infiles:
    print "Start processing: ", f
    inf = os.path.join(inpath,f)
    get_hdf(inf,outdir)











