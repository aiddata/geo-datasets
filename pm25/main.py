# for converting MONTHLY pm data downloaded from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148055008434 
# version: multiple file download - HPC version, based off Dr. Goodman's script for converting nc file to tiff image, MONTHLY data

import warnings
import os
import pandas as pd

import rasterio
from affine import Affine
from netCDF4 import Dataset
import numpy as np

from utils import get_current_timestamp, file_exists, export_raster, run_tasks

# -------------------------------------
#CHANGE VAR
input_dir = "/home/jacob/Documents/geo-datasets/pm25/output_data"
input_path_template = "V5GL02.HybridPM25.Global.{YEAR}{MONTH}-{YEAR}{MONTH}.nc"

#CONSIDER: Changing the output file name to something cleaner
#CHANGE VAR
input_dir = "/home/jacob/Documents/geo-datasets/pm25/output_data"
output_path_template = "V5GL02.HybridPM25.Global.{YEAR}{MONTH}-{YEAR}{MONTH}.tif"

#CHANGE VAR
year_list = range(2003, 2004)

#CHANGE VAR
month_list = range(1, 3)

timestamp = get_current_timestamp("%Y_%m_%d_%H_%M")

#CHANGE VAR: adjust based on whether you want parallel processing
run_parallel = True

#CHANGE VAR: adjust based on system's maximum workers
max_workers = 4
# -------------------------------------

def convert_file(input_path, output_path):
    #converts nc file to tiff file, compatible with parallel processing system
    overwrite = False
    if os.path.isfile(output_path) and not overwrite:
        return (output_path, "Exists", 0)
    try:
        rootgrp = Dataset(input_path, "r", format="NETCDF4")

        lon_min = rootgrp.variables["lon"][:].min()
        lon_max = rootgrp.variables["lon"][:].max()
        lon_size = len(rootgrp.variables["lon"][:])
        lon_res = rootgrp.variables["lon"][1] - rootgrp.variables["lon"][0]
        lon_res_true = 0.0099945068359375

        lat_min = rootgrp.variables["lat"][:].min()
        lat_max = rootgrp.variables["lat"][:].max()
        lat_size = len(rootgrp.variables["lat"][:])
        lat_res_true = 0.009998321533203125
        lat_res = rootgrp.variables["lat"][1] - rootgrp.variables["lat"][0]

        data = np.flip(rootgrp.variables["GWRPM25"][:], axis=0)

        meta = {
            "driver": "GTiff",
            "dtype": "float32",
            "nodata": data.fill_value,
            "width": lon_size,
            "height": lat_size,
            "count": 1,
            "crs": {"init": "epsg:4326"},
            "compress": "lzw",
            "transform": Affine(lon_res, 0.0, lon_min,
                                0.0, -lat_res, lat_max)
            }


        export_raster(np.array([data.data]), output_path, meta)
        
        return(output_path, "Converted", 0)
    except Exception as e:
        return(output_path, repr(e), 1)

# -------------------------------------
if __name__ == "__main__":

    print("Preparing Data Conversion:")

    os.makedirs(output_dir, exist_ok=True)

    input_path_list = []
    output_path_list = []
    #CONSIDER: find a way to set each year's month range individually so if researcher wants different months for each year can adjust
    for year in year_list:
        for month in month_list:
            if month < 10:
                month = "0" + str(month)
            input_path = input_path_template.format(YEAR = year, MONTH = month)
            input_path_list.append(os.path.join(input_dir, input_path))

            output_path = output_path_template.format(YEAR = year, MONTH = month)
            output_path_list.append(os.path.join(output_dir, output_path))
    df = pd.DataFrame({"input_file_path": input_path_list, "output_file_path": output_path_list})
    flist = list(zip(df["input_file_path"], df["output_file_path"]))

    print("Running Data Conversion:")

    results = run_tasks(convert_file, flist, run_parallel, max_workers=max_workers, chunksize=1)

    print("Results:")
    
    results_df = pd.DataFrame(results, columns=["output_file_path", "message", "status"])

    output_df = df.merge(results_df, on="output_file_path", how="left")
    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    os.makedirs(os.path.join(output_dir, "results"), exist_ok=True)
    output_csv_path = os.path.join(output_dir, "results", f"data_conversion_{timestamp}.csv")
    output_df.to_csv(output_csv_path, index=False)
