
try:
    from mpi4py import MPI

    # mpi info
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    run_mpi = True

except:
    size = 1
    rank = 0
    run_mpi = False


import os
import sys
import numpy as np
from osgeo import gdal
from osgeo import gdal_array
from osgeo import osr
import math

# --------------------


data_path = "/sciclone/aiddata10/REU/data/rasters/external/global/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"
out_base = "/sciclone/aiddata10/REU/data/rasters/external/global/modis_yearly"


method = "max"

# nodata = -9999
nodata = 255

# --------------------


method_list = ["max", "mean", "var"]

if method not in method_list:
    sys.exit("Bad method given")


# list of years to ignore/accept
# list of all years to process

ignore = ['2000']
qlist = [name for name in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, name)) and name not in ignore]

# accept = ['2000']
# qlist = [name for name in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, name)) and name in accept]
        


c = rank

while c < len(qlist):

    year = qlist[c]
    print year
    
    file_paths = [data_path + "/" + year + "/" + name for name in os.listdir(data_path +"/"+ year) if not os.path.isdir(os.path.join(data_path +"/"+ year, name)) and name.endswith(".tif")]


    try:
        year_files = [gdal.Open(f) for f in file_paths]

    except:
        print "error opening files"
        c += size
        continue



    # ncols = 160000
    # nrows = 64000

    # year_data = np.empty([nrows, ncols], dtype='uint8')


    kfw_sample_bounds = {
        'xMin': -72.5713,
        'xMax': -48.3943,
        'yMax': 4.02843,
        'yMin': -11.1633
    }


    year_data = []

    # try:

    for i in range(len(year_files)):
        tmp_file = year_files[i]
        tmp_geotransform = tmp_file.GetGeoTransform()


        col_start = int(math.floor((kfw_sample_bounds['xMin'] - tmp_geotransform[0] ) / tmp_geotransform[1]))
        col_end = int(math.ceil((kfw_sample_bounds['xMax'] - tmp_geotransform[0] ) / tmp_geotransform[1]))
        row_start = int(math.floor((tmp_geotransform[3] - kfw_sample_bounds['yMax']) / abs(tmp_geotransform[5])))
        row_end = int(math.ceil((tmp_geotransform[3] - kfw_sample_bounds['yMin']) / abs(tmp_geotransform[5])))
        
        col_size = col_end - col_start
        row_size = row_end - row_start

        # print (col_start,col_end,row_start,row_end, col_size, row_size)

        year_read = np.array(tmp_file.GetRasterBand(1).ReadAsArray(col_start, row_start, col_size, row_size))
        year_data.append(year_read)



    year_data = np.array(year_data)
    year_masked = np.ma.MaskedArray(year_data, mask=year_data==nodata) 
    year_output = year_masked.max(axis=0)
    year_output = year_output.filled(nodata)


    # except:
    #     print "error running calc"
    #     c += size
    #     continue

    output_geotransform = (-72.57225, 0.00225, 0.0, 4.02975, 0.0, -0.00225)

    try:
        output_path = out_base +"/"+ method +"/kfw/"+ year + ".tif"
        output_raster = gdal.GetDriverByName('GTiff').Create(output_path, year_output.shape[1], year_output.shape[0], 1 , gdal.GDT_Byte)  
        output_raster.SetGeoTransform(output_geotransform)  
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        output_raster.SetProjection(srs.ExportToWkt()) 
        output_raster.GetRasterBand(1).SetNoDataValue(nodata)
        output_raster.GetRasterBand(1).WriteArray(year_output)
    except:
        print "error writing output"
        c += size
        continue


    c += size

