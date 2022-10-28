import os
import shutil
import requests
from zipfile import ZipFile
from copy import copy
import time
import datetime
import rasterio
from rasterio import windows



def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def download_file(url, local_filename):
    """Download a file from url to local_filename
    Downloads in chunks
    """
    with requests.get(url, stream=True, verify=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                f.write(chunk)


def manage_download(url, local_filename, overwrite=False):
    """download individual file using session created
    this needs to be a standalone function rather than a method
    of SessionWithHeaderRedirection because we need to be able
    to pass it to our mpi4py map function
    """
    max_attempts = 5
    if os.path.isfile(local_filename) and not overwrite:
        print(f'Download Exists: {url}')
    else:
        attempts = 1
        while attempts <= max_attempts:
            try:
                download_file(url, local_filename)
                print(f"Downloaded: {url}")
            except Exception as e:
                attempts += 1
                if attempts > max_attempts:
                    raise e


def copy_files(zip_path, zip_file, dst_path, overwrite=False):
    if os.path.isfile(dst_path) and not overwrite:
        return dst_path

    else:
        try:
            with ZipFile(zip_path) as myzip:
                with myzip.open(zip_file) as src:
                    with open(dst_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)

            if not os.path.isfile(dst_path):
                raise Exception("File extracted but not found at destination")

            return dst_path
        except Exception as e:
            raise e


def convert_to_cog(src_path, dst_path):
    '''Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
    '''
    with rasterio.open(src_path, 'r') as src:

        profile = copy(src.profile)

        profile.update({
            'driver': 'COG',
            'compress': 'LZW',
        })

        with rasterio.open(dst_path, 'w+', **profile) as dst:

            for ji, src_window in src.block_windows(1):
                # convert relative input window location to relative output window location
                # using real world coordinates (bounds)
                src_bounds = windows.bounds(src_window, transform=src.profile["transform"])
                dst_window = windows.from_bounds(*src_bounds, transform=dst.profile["transform"])
                # round the values of dest_window as they can be float
                dst_window = windows.Window(round(dst_window.col_off), round(dst_window.row_off), round(dst_window.width), round(dst_window.height))
                # read data from source window
                r = src.read(1, window=src_window)
                # write data to output window
                dst.write(r, 1, window=dst_window)


def task(zip_path, zip_file, tif_path, cog_path):

    _ = copy_files(zip_path, zip_file, tif_path)

    convert_to_cog(tif_path, cog_path)

    return cog_path