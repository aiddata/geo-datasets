
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


coef = COEFFICIENTS['ELVIDGE2014']


data_path = '/sciclone/aiddata10/REU/data/rasters/external/global/v4composites'

qlist = [name for name in os.listdir(data_path) if not os.path.isdir(os.path.join(data_path, name)) and name.endswith('.tif')]



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

    print sensor +' '+ year

    tmp_coefs = coef[sensor][year]
    c0 = tmp_coefs[0]
    c1 = tmp_coefs[1]
    c2 = tmp_coefs[2]


    file_path = os.path.join(data_path, i)


    tmp_file = gdal.Open(file_path)
    ncols = tmp_file.RasterXSize
    nrows = tmp_file.RasterYSize
    geotransform = tmp_file.GetGeoTransform()


    input_array = np.array(tmp_file.GetRasterBand(1).ReadAsArray())
    

    # =============

    # tmp_array = input_array
    # tmp_array[np.where(input_array == 0)] = 1

    # output_array = c0 + (tmp_array * c1) + (tmp_array ** c2)

    # output_array = np.int8(np.round(output_array))
    # output_array[np.where(input_array == 0)] = 0
    # output_array[np.where(output_array > 63)] = 63

    # =============

    tmp_array = np.ma.MaskedArray(input_array, mask=input_array==0)

    output_array = c0 + (tmp_array * c1) + (tmp_array ** c2)

    output_array = np.int8(np.round(output_array))
    output_array = output_array.filled(0)
    output_array[np.where(output_array > 63)] = 63



    out_base = '/sciclone/aiddata10/REU/data/rasters/external/global/v4composites_calibrated'

    make_dir(out_base)

    output_path = out_base +"/"+ i[0:-4] + '_calibrated.tif'
    

    output_raster = gdal.GetDriverByName('GTiff').Create(output_path, ncols, nrows, 1 , gdal.GDT_Byte )  
    output_raster.SetGeoTransform(geotransform)  
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    output_raster.SetProjection(srs.ExportToWkt()) 
    output_raster.GetRasterBand(1).WriteArray(output_array)


    T_init = int(time.time() - Ts)
    print('Single Calibration Runtime: ' + str(T_init//60) +'m '+ str(int(T_init%60)) +'s')

