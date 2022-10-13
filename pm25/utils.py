"""Functions and classes for converting pm concentration data from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148054977849
"""

import os
import time
import datetime
import warnings
import rasterio


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp

def file_exists(path):
    return os.path.isfile(path)

def run_tasks(func, flist, mode, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)
    if mode == "parallel":

        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor

        if max_workers is None:

            if "OMPI_UNIVERSE_SIZE" not in os.environ:
                raise ValueError("Mode set to parallel but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")

            max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
            warnings.warn(f"Mode set to parallel but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")

        with MPIPoolExecutor(max_workers=max_workers) as executor:
            results_gen = executor.starmap(func, flist, chunksize=chunksize)

        results = list(results_gen)

    else:
        results = []
        for i in flist:
            results.append(func(*i))

    return results

                
def export_raster(data, path, meta, **kwargs):
    """Export raster array to geotiff
    """
    if not isinstance(meta, dict):
        raise ValueError("meta must be a dictionary")

    if 'dtype' in meta:
        if meta["dtype"] != data.dtype:
            warnings.warn(f"Dtype specified by meta({meta['dtype']}) does not match data dtype ({data.dtype}). Adjusting data dtype to match meta.")
        data = data.astype(meta["dtype"])
    else:
        meta['dtype'] = data.dtype

    default_meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'driver': 'GTiff',
        'compress': 'lzw',
        'nodata': -9999,
    }

    for k, v in default_meta.items():
        if k not in meta:
            if 'quiet' not in kwargs or kwargs["quiet"] == False:
                print(f"Value for `{k}` not in meta provided. Using default value ({v})")
            meta[k] = v

    # write geotif file
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(data)