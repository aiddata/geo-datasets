"""
Author: Miranda Lv
Date: 06/21/2018
This script is used to:
    - read hdf data files and extract day/night land surface temperature.
    - the raw data was in 3600*7200 matrix, and there is no re-projection.
"""

import os
import errno
import rasterio
import numpy as np
from affine import Affine
from datetime import datetime
from pyhdf.SD import SD, SDC



def make_dir(path):
    """Make directory.

    Args:
        path (str): absolute path for directory

    Raise error if error other than directory exists occurs.
    """
    if path != '':
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise


def export_raster(raster, affine, path, out_dtype='float64', nodata=None):
    """Export raster array to geotiff
    """
    # affine takes upper left
    # (writing to asc directly used lower left)
    meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'dtype': out_dtype,
        'affine': affine,
        'driver': 'GTiff',
        'height': raster.shape[0],
        'width': raster.shape[1],
        # 'nodata': -1,
        # 'compress': 'lzw'
    }

    if nodata is not None:
        meta['nodata'] = nodata

    raster_out = np.array([raster.astype(out_dtype)])

    make_dir(os.path.dirname(path))

    # write geotif file
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(raster_out)



def get_time(datestring):

    datatime = datetime.strptime(datestring, "%Y%j")

    year = "%d"%datatime.year
    mon = "%02d"%datatime.month

    newtime = str(year) + "_" + str(mon)

    return newtime


def get_hdf(infile, output_dir):

    # get data time stamp
    datestring = infile.split(".")[1][1:]
    newtime = get_time(datestring)

    # get new data export name
    dayout = os.path.join(output_dir, "modis_lst_day_cmg_" + newtime + ".tif")
    nightout = os.path.join(output_dir, "modis_lst_night_cmg_" + newtime + ".tif")

    # read hdf data files
    file = SD(infile, SDC.READ)

    day_img = file.select('LST_Day_CMG')
    day_dta = day_img.get()
    day_dta = day_dta * scale_factor
    export_raster(day_dta, affine, dayout, nodata=0)

    night_img = file.select('LST_Night_CMG')
    night_dta = night_img.get()
    night_dta = night_dta * scale_factor
    export_raster(night_dta, affine, nightout, nodata=0)




scale_factor = 0.02

pixel_size = 0.05

affine = Affine(pixel_size, 0, -180,
                0, -pixel_size, 90)


input_dir = "/sciclone/aiddata10/REU/geo/raw/modis_lst/MOD11C3"

output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/modis_lst/daily"


file_list = [
    f for f in os.listdir(input_dir)
    if f.endswith(".hdf")
]


for f in file_list:
    print "Processing: ", f
    fpath = os.path.join(input_dir, f)
    get_hdf(fpath, output_dir)






