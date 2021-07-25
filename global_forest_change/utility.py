import os
import copy
import time
import math
import datetime
import warnings
import requests
import rasterio
# from rasterio.merge import merge
from rasterio import Affine, windows

def file_exists(path):
    return os.path.isfile(path)


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def download_file(url, local_filename):
    """Download a file from url to local_filename

    Downloads in chunks
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                f.write(chunk)



# def create_mosaic(tile_list):
#     """Basic mosaic function (not memory efficient)
#     """
#     mosaic_scenes = [rasterio.open(path) for path in tile_list]
#     meta = copy.copy(mosaic_scenes[0].meta)

#     data, transform = merge(mosaic_scenes)

#     for i in mosaic_scenes: i.close()

#     if 'affine' in meta:
#         meta.pop('affine')

#     meta["transform"] = transform
#     meta['height'] = data.shape[1]
#     meta['width'] = data.shape[2]
#     meta['driver'] = 'GTiff'

#     return data, meta



def create_mosaic(tile_list, output_path):
    """Memory efficient mosaic function
    Based on code from:
    - https://gis.stackexchange.com/questions/348925/merging-rasters-with-rasterio-in-blocks-to-avoid-memoryerror
    - https://github.com/mapbox/rasterio/blob/master/rasterio/merge.py
    - https://github.com/mapbox/rio-merge-rgba/blob/master/merge_rgba/__init__.py
    """
    sources = [rasterio.open(raster) for raster in tile_list]

    res = sources[0].res
    nodata = sources[0].nodata
    dtype = sources[0].dtypes[0]
    output_count = sources[0].count

    # Extent of all inputs
    # scan input files
    xs = []
    ys = []
    for src in sources:
        left, bottom, right, top = src.bounds
        xs.extend([left, right])
        ys.extend([bottom, top])

    dst_w, dst_s, dst_e, dst_n = min(xs), min(ys), max(xs), max(ys)

    out_transform = Affine.translation(dst_w, dst_n)

    # Resolution/pixel size
    out_transform *= Affine.scale(res[0], -res[1])

    # Compute output array shape. We guarantee it will cover the output bounds completely
    output_width = int(math.ceil((dst_e - dst_w) / res[0]))
    output_height = int(math.ceil((dst_n - dst_s) / res[1]))

    # Adjust bounds to fit
    # dst_e, dst_s = out_transform * (output_width, output_height)

    # create destination array
    # destination array shape
    shape = (output_height, output_width)

    dest_profile = {
        "driver": 'GTiff',
        "height": shape[0],
        "width": shape[1],
        "count": output_count,
        "dtype": dtype,
        "crs": '+proj=latlong',
        "transform": out_transform,
        "compress": "LZW",
        "tiled": True,
        "nodata": nodata,
        "bigtiff": True,
    }

    # open output file in write/read mode and fill with destination mosaick array
    with rasterio.open(output_path, 'w+', **dest_profile) as mosaic_raster:
        for src in sources:
            for ji, src_window in src.block_windows(1):
                # convert relative input window location to relative output window location
                # using real world coordinates (bounds)
                src_bounds = windows.bounds(src_window, transform=src.profile["transform"])
                dst_window = windows.from_bounds(*src_bounds, transform=mosaic_raster.profile["transform"])
                # round the values of dest_window as they can be float
                dst_window = windows.Window(round(dst_window.col_off), round(dst_window.row_off), round(dst_window.width), round(dst_window.height))
                # read data from source window
                r = src.read(1, window=src_window)
                # if tiles/windows have overlap:
                # before writing the window, replace source nodata with dest nodata as it can already have been written
                # dest_pre = mosaic_raster.read(1, window=dst_window)
                # mask = (np.isnan(r))
                # r[mask] = dest_pre[mask]
                # write data to output window
                mosaic_raster.write(r, 1, window=dst_window)



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


def _task_wrapper(func, args):
    try:
        func(*args)
        return (0, "Success", args)
    except Exception as e:
        return (1, repr(e), args)


def run_tasks(func, flist, mode, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)

    wrapper_list = [(func, i) for i in flist]

    if mode == "parallel":

        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor

        if max_workers is None:

            if "OMPI_UNIVERSE_SIZE" not in os.environ:
                raise ValueError("Mode set to parallel but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")

            max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
            warnings.warn(f"Mode set to parallel but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")

        with MPIPoolExecutor(max_workers=max_workers) as executor:
            # results_gen = executor.starmap(func, flist, chunksize=chunksize)
            results_gen = executor.starmap(_task_wrapper, wrapper_list, chunksize=chunksize)

        results = list(results_gen)

    else:
        results = []
        # for i in flist:
            # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))

    return results
