
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
        

geotransform = None


c = rank

while c < len(qlist):

    year = qlist[c]
    print year
    
    file_paths = [data_path + "/" + year + "/" + name for name in os.listdir(data_path +"/"+ year) if not os.path.isdir(os.path.join(data_path +"/"+ year, name)) and name.endswith(".tif")]


    try:
        all_year_files = [gdal.Open(f) for f in file_paths]

    except:
        print "error opening files"
        c += size
        continue


    ncols = 160000
    nrows = 64000

    year_files = [f for f in all_year_files if f.RasterXSize == ncols and f.RasterYSize == nrows]

    if geotransform == None:
        tmp_file = year_files[0]
        geotransform = tmp_file.GetGeoTransform()


    year_data = np.empty([nrows, ncols], dtype='uint8')


    try:
        for i in range(nrows):

            # print str(year) +" "+ str(i)

            row_read = np.array([y.GetRasterBand(1).ReadAsArray(0, i, ncols, 1) for y in year_files])

            # row_read[np.where(row_read == 255)] = nodata
            # row_data = np.max(row_read, axis=0)
            # year_data[i] = row_data

            row_masked = np.ma.MaskedArray(row_read, mask=row_read==nodata) 
            row_data = row_masked.max(axis=0)
            year_data[i] = row_data.filled(nodata)

    except:
        print "error running calc"
        c += size
        continue


    try:
        output_path = out_base +"/"+ method +"/"+ year + ".tif"
        output_raster = gdal.GetDriverByName('GTiff').Create(output_path, ncols, nrows, 1 , gdal.GDT_Byte)  
        output_raster.SetGeoTransform(geotransform)  
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        output_raster.SetProjection(srs.ExportToWkt()) 
        output_raster.GetRasterBand(1).SetNoDataValue(nodata)
        output_raster.GetRasterBand(1).WriteArray(year_data)
    except:
        print "error writing output"
        c += size
        continue


    c += size

