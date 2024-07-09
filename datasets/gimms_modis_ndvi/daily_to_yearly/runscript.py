# originally ndviCalmpi2.py

from mpi4py import MPI
import os
import sys
import numpy as np
from osgeo import gdal
from osgeo import gdal_array
from osgeo import osr


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
status = MPI.Status()


# --------------------

data_path = "/sciclone/aiddata10/REU/data/rasters/external/global/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"
out_base = "/sciclone/aiddata10/REU/data/rasters/external/global/modis_yearly"


method = "max"

nodata = -9999

# --------------------


# verify data_path contains daily data
# 

# verify out_base exists
# 

# create method folder in out_base if it does not exists
# 


def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


tags = enum('READY', 'DONE', 'EXIT', 'START')

method_list = ["max", "mean", "var"]

if method not in method_list:
    sys.exit("Bad method given")


# list of years to ignore/accept
# list of all years to process

# ignore = []
# qlist = [name for name in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, name)) and name not in ignore]

accept = ['2000']
qlist = [name for name in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, name)) and name in accept]
        

for i in range(len(qlist)): 

    year = qlist[i]

    filelist = [data_path + "/" + year + "/" + name for name in os.listdir(data_path +"/"+ year) if not os.path.isdir(os.path.join(data_path +"/"+ year, name)) ]

    if rank ==0:

        geotransform = None
        tmp_file = gdal.Open(filelist[1])
        ncols = tmp_file.RasterXSize
        nrows = tmp_file.RasterYSize
        # tmp_array = np.array(tmp_file.GetRasterBand(1).ReadAsArray())
        # nrows, ncols = np.shape(tmp_array)
        geotransform = tmp_file.GetGeoTransform()

        year_data = tmp_array
        year_data[year_data == 255] = nodata

        tasks = filelist
        task_index = 0
        num_workers = size - 1
        closed_workers = 0

        

        print("Master starting with %d workers" % num_workers)

        while closed_workers < num_workers:
            data = np.empty([nrows, ncols], dtype='uint8')
            comm.Recv([data, MPI.INT], source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            
            data[data == 255] = nodata

            if tag == tags.READY:
                if task_index < len(tasks):
                    comm.send(tasks[task_index], dest=source, tag=tags.START)
                    print("Sending task %d to worker %d" % (task_index, source))
                    task_index += 1

                else:
                    comm.send(None, dest=source, tag=tags.EXIT)

            elif tag == tags.DONE:
                year_data = np.max([year_data, data], axis=0)
                print("Got data from worker %d" % source)

            elif tag == tags.EXIT:
                print("Worker %d exited." % source)
                closed_workers +=1


        if geotransform != None:
            output_path = out_base +"/"+ method +"/"+ year + ".tif"
            output_raster = gdal.GetDriverByName('GTiff').Create(output_path, ncols, nrows, 1 , gdal.GDT_Int16)  
            output_raster.SetGeoTransform(geotransform)  
            srs = osr.SpatialReference()                 
            srs.ImportFromEPSG(4326)  
            output_raster.SetProjection(srs.ExportToWkt()) 
            output_raster.GetRasterBand(1).SetNoDataValue(nodata)
            output_raster.GetRasterBand(1).WriteArray(year_data)


    else:

        name = MPI.Get_processor_name()
        print("Worker rank %d on %s." % (rank, name))

        while True:
            comm.send(None,dest=0,tag=tags.READY)
            task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            if tag == tags.START:
                ds = gdal.Open(task)
                myarray = np.array(ds.GetRasterBand(1).ReadAsArray())
                comm.Isend([myarray, MPI.INT], dest=0, tag=tags.DONE)

            if tag == tags.EXIT:
                comm.Isend([0, MPI.INT], dest=0, tag=tags.EXIT)
                break


