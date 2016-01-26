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

data_path = "/sciclone/aiddata10/REU/data/ltdr.nascom.nasa.gov/allData/Ver4/ndvi"

out_base = "/sciclone/aiddata10/REU/data/ltdr_yearly"

method = "mean"

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

# ignore = ['1981']
# qlist = [name for name in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, name)) and name not in ignore]

accept = ['1981']
qlist = [name for name in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, name)) and name in accept]
        

for i in range(len(qlist)): 

    year = qlist[i]

    filelist = [data_path + "/" + year + "/" + name for name in os.listdir(data_path +"/"+ year) if not os.path.isdir(os.path.join(data_path +"/"+ year, name)) ]

    if rank ==0:

        geotransform = None
        tmp_file = gdal.Open(filelist[1])
        tmp_array = np.array(tmp.GetRasterBand(1).ReadAsArray())
        nrows,ncols = np.shape(tmp_array)
        geotransform = tmp_file.GetGeoTransform()

        year_data=[]
        tasks = filelist
        task_index = 0
        num_workers = size - 1
        closed_workers = 0

        print("Master starting with %d workers" % num_workers)

        while closed_workers < num_workers:
            data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            
            if tag == tags.READY:
                if task_index < len(tasks):
                    comm.send(tasks[task_index], dest=source, tag=tags.START)
                    print("Sending task %d to worker %d" % (task_index, source))
                    task_index += 1

                else:
                    comm.send(None, dest=source, tag=tags.EXIT)

            elif tag == tags.DONE:
                year_data.append(data)
                print("Got data from worker %d" % source)

            elif tag == tags.EXIT:
                print("Worker %d exited." % source)
                closed_workers +=1


        year_data = np.array(year_data)
        # masked_year_data = np.ma.MaskedArray(year_data, np.in1d(year_data, [nodata]), fill_value=nodata)

        if method == "max":
            result = np.max(year_data, axis=0)

        elif method == "mean":
            year_data = np.array(year_data, dtype=np.float32)
            year_data[year_data == -9999] = np.nan
            result = np.nanmean(year_data, axis=0)
            result[np.isnan(result)] = float(nodata)
            # result = np.ma.mean(masked_year_data, axis=0).filled(nodata)

        elif method == "var":
            # result = np.ma.var(masked_year_data, axis=0).filled(nodata)
            year_data = np.array(year_data, dtype=np.float32)
            year_data[year_data == -9999] = np.nan
            result = np.nanvar(year_data, axis=0)
            result[np.isnan(result)] = float(nodata)


        # result = result[np.isnan(result)] = nodata

        # if np.nan in np.ravel(result):
        #     sys.exit("NOT A NUMBER IS PRESENT IN result ARRAY")


        if geotransform != None:
            output_path = out_base +"/"+ method +"/"+ year + ".tif"
            output_raster = gdal.GetDriverByName('GTiff').Create(output_path,ncols, nrows, 1 ,gdal.GDT_Float32)  
            output_raster.SetGeoTransform(geotransform)  
            srs = osr.SpatialReference()                 
            srs.ImportFromEPSG(4326)  
            output_raster.SetProjection(srs.ExportToWkt()) 
            output_raster.GetRasterBand(1).SetNoDataValue(nodata)
            output_raster.GetRasterBand(1).WriteArray(result)


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
                comm.send(myarray, dest=0, tag=tags.DONE)

            if tag == tags.EXIT:
                comm.send(None, dest=0, tag=tags.EXIT)
                break

