"""
Functions and classes for converting pm concentration data from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148054977849
"""

import os
import time
import datetime
import warnings
import rasterio
import numpy as np
from affine import Affine
from netCDF4 import Dataset

use_prefect = False

def get_current_timestamp(format_str="'%Y_%m_%d_%H_%M"):
    return datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)



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

                
def export_raster(data, path, meta, **kwargs):
    """
    Export raster array to geotiff
    """
    if not isinstance(meta, dict):
        raise ValueError("meta must be a dictionary")

    if "dtype" in meta:
        if meta["dtype"] != data.dtype:
            warnings.warn(f"Dtype specified by meta({meta['dtype']}) does not match data dtype ({data.dtype}). Adjusting data dtype to match meta.")
        data = data.astype(meta["dtype"])
    else:
        meta["dtype"] = data.dtype

    default_meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'driver': 'GTiff',
        'compress': 'lzw',
        'nodata': -9999,
    }

    for k, v in default_meta.items():
        if k not in meta:
            if "quiet" not in kwargs or kwargs["quiet"] == False:
                print(f"Value for `{k}` not in meta provided. Using default value ({v})")
            meta[k] = v

    # write geotif file
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(data)
