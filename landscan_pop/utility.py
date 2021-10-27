
import os
import time
import datetime
import warnings
import zipfile
import requests

import rasterio

session = requests.Session()

def download_file(year, local_filename):
    overwrite = False
    if os.path.isfile(local_filename) and not overwrite:
        return "Exists"
    base_url = "https://landscan.ornl.gov/system/files"
    # some source files have nonstandard names (e.g. "landscan_2000_0.zip" instead of "landscan_2000.zip")
    # these variables and the while loop support iterating through potential variations to find correct file
    extra_str = ""
    extra_int = 0
    while True:
        dl_link = f"{base_url}/LandScan%20Global%20{year}{extra_str}.zip"
        # download server seems prone to request errors, this allows multiple attempts
        attempts = 0
        while attempts < 5:
            try:
                response = session.get(dl_link, stream=True)
                break
            except:
                attempts += 1
                time.sleep(5)

        if response.status_code == 200:
            with open(local_filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            response.close()
            return "Downloaded"
        else:
            # indicates the current file we are attempting is wrong
            response.close()
            if extra_int >= 3:
                raise Exception(f"Could not find valid download link for year {year}")
            extra_str = f"_{extra_int}"
            extra_int += 1


def unzip_file(zip_file, out_dir):
    """Extract a zipfile"""
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(out_dir)


def convert_esri_grid_to_geotiff(esri_grid_path, geotiff_path):
    """Convery a raster from ESRI grid format to GeoTIFF format"""
    with rasterio.open(esri_grid_path) as src:
        assert len(set(src.block_shapes)) == 1
        meta = src.meta.copy()
        meta.update(driver="GTIFF")
        with rasterio.open(geotiff_path, "w", **meta) as dst:
            for ji, window in src.block_windows(1):
                in_data = src.read(window=window)
                dst.write(in_data, window=window)


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def _task_wrapper(func, args):
    try:
        result = func(*args)
        return (0, "Success", args, result)
    except Exception as e:
        # raise
        return (1, repr(e), args, None)


def run_tasks(func, flist, parallel, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)
    wrapper_list = [(func, i) for i in flist]
    if parallel:
        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        # and: https://docs.python.org/3/library/concurrent.futures.html
        try:
            from mpi4py.futures import MPIPoolExecutor
            mpi = True
        except:
            from concurrent.futures import ProcessPoolExecutor
            mpi = False
        if max_workers is None:
            if mpi:
                if "OMPI_UNIVERSE_SIZE" not in os.environ:
                    raise ValueError("Parallel set to True and mpi4py is installed but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")
                max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
                warnings.warn(f"Parallel set to True (mpi4py is installed) but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")
            else:
                import multiprocessing
                max_workers = multiprocessing.cpu_count()
                warnings.warn(f"Parallel set to True (mpi4py is not installed) but max_workers not specified. Defaulting to CPU count ({max_workers})")
        if mpi:
            with MPIPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.starmap(_task_wrapper, wrapper_list, chunksize=chunksize)
        else:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.map(_task_wrapper, *zip(*wrapper_list), chunksize=chunksize)
        results = list(results_gen)
    else:
        results = []
        # for i in flist:
            # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))
    return results
