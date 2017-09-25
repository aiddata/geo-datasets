
import time
import os
import sys
import errno
import numpy as np
from osgeo import gdal
from osgeo import gdal_array
from osgeo import osr


# intercalibration_coefficients from:
# https://gitlab.com/NikosAlexandris/i.nightlights.intercalibration/blob/master/intercalibration_coefficients.py
from intercalibration_coefficients import COEFFICIENTS



src_dir = '/sciclone/aiddata10/REU/geo/data/rasters/dmsp_ntl/v4composites'
dst_dir = '/sciclone/aiddata10/REU/geo/data/rasters/dmsp_ntl/v4composites_calibrated_201709'

coef = COEFFICIENTS['ELVIDGE2014']


qlist = [name for name in os.listdir(src_dir)
         if not os.path.isdir(os.path.join(src_dir, name))
         and name.endswith('.tif')]



# creates directories
def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise



for i in qlist:

    Ts = int(time.time())

    sensor = i[0:3]
    year = i[3:7]

    # if int(year) != 2013:
    #     continue

    print sensor +' '+ year


    tmp_coefs = coef[sensor][year]
    c0 = tmp_coefs[0]
    c1 = tmp_coefs[1]
    c2 = tmp_coefs[2]


    file_path = os.path.join(src_dir, i)


    tmp_file = gdal.Open(file_path)
    ncols = tmp_file.RasterXSize
    nrows = tmp_file.RasterYSize
    geotransform = tmp_file.GetGeoTransform()


    input_array = np.array(tmp_file.GetRasterBand(1).ReadAsArray())


    # =============

    # tmp_array = input_array
    # tmp_array[np.where(input_array == 0)] = 1

    # output_array = c0 + (c1 * tmp_array) + (c2 * tmp_array ** 2)

    # output_array = np.int8(np.round(output_array))
    # output_array[np.where(input_array == 0)] = 0
    # output_array[np.where(output_array > 63)] = 63

    # =============

    tmp_array = np.ma.MaskedArray(input_array, mask=input_array==0)

    output_array = c0 + (c1 * tmp_array) + (c2 * tmp_array ** 2)

    output_array[np.where(output_array > 63)] = 63
    output_array = np.uint8(np.round(output_array))

    output_array[np.where(tmp_array == 255)] = 255

    output_array = output_array.filled(0)




    make_dir(dst_dir)

    output_path = dst_dir +"/"+ i[0:-4] + '_calibrated.tif'


    output_raster = gdal.GetDriverByName('GTiff').Create(output_path, ncols, nrows, 1 , gdal.GDT_Byte )
    output_raster.SetGeoTransform(geotransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    output_raster.SetProjection(srs.ExportToWkt())
    output_raster.GetRasterBand(1).SetNoDataValue(255)
    output_raster.GetRasterBand(1).WriteArray(output_array)

    del output_raster

    T_init = int(time.time() - Ts)
    print('Single Calibration Runtime: ' + str(T_init//60) +'m '+ str(int(T_init%60)) +'s')

