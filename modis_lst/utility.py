"""Functions and classes for processing MODIS land surface temperature data
"""

import os
import time
import datetime
import warnings
import requests
import rasterio
import numpy as np
from bs4 import BeautifulSoup
from pyhdf.SD import SD, SDC


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def run_tasks(func, flist, mode, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)
    if mode == "parallel":

        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor

        if max_workers is None:

            if "OMPI_UNIVERSE_SIZE" not in os.environ:
                raise ValueError("Mode set to parallel but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")

            max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
            warning.warn(f"Mode set to parallel but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")

        with MPIPoolExecutor(max_workers=max_workers) as executor:
            results_gen = executor.starmap(func, flist, chunksize=chunksize)

        results = list(results_gen)

    else:
        results = []
        for i in flist:
            results.append(func(*i))

    return results


def listFD(url, ext=''):
    """Find all links in a webpage

    Option matching on string at end of links founds

    Returns list of complete (absolute) links
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    urllist = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return urllist


def get_hdf_url(url):
    """Find links for HDF files within given url
    """
    hdfurl = listFD(url, 'hdf')
    if len(hdfurl) == 0:
        return "Error"
    else:
        return hdfurl[0]


class SessionWithHeaderRedirection(requests.Session):
    """overriding requests.Session.rebuild_auth to mantain headers when redirected
    from: https://wiki.earthdata.nasa.gov/display/EL/How+To+Access+Data+With+Python
    """
    AUTH_HOST = 'urs.earthdata.nasa.gov'
    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)
    def rebuild_auth(self, prepared_request, response):
        """Overrides from the library to keep headers when redirected to or from
        the NASA auth host.
        """
        headers = prepared_request.headers
        url = prepared_request.url
        if 'Authorization' in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.AUTH_HOST and \
                    original_parsed.hostname != self.AUTH_HOST:
                del headers['Authorization']
        return


def get_temporal(x):
    """ converts a string from year-day to year-month
    input format:  YYYYDDD
    output format: YYYY_MM
    """
    datetime_obj = datetime.datetime.strptime(x, "%Y%j")
    year = "%d"%datetime_obj.year
    month = "%02d"%datetime_obj.month
    return "{}_{}".format(year, month)



def load_hdf(path, layer):
    """read hdf data files
    """
    file = SD(path, SDC.READ)
    img = file.select(layer)
    data = img.get() * img.attributes()["scale_factor"]
    return data


def export_raster(data, path, meta, **kwargs):
    """Export raster array to geotiff
    """
    if not isinstance(meta, dict):
        raise ValueError("meta must be a dictionary")

    if 'dtype' in meta:
        if meta["dtype"] != data.dtype:
            warning.warn(f"Dtype specified by meta({meta['dtype']}) does not match data dtype ({data.dtype}). Adjusting data dtype to match meta.")
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



def aggregate_rasters(file_list, method="mean"):
    """Aggregate multiple rasters

    Aggregates multiple rasters with same features (dimensions, transform,
    pixel size, etc.) and creates single layer using aggregation method
    specified.

    Supported methods: mean (default), max, min, sum

    Arguments
        file_list (list): list of file paths for rasters to be aggregated
        method (str): method used for aggregation

    Return
        result: rasterio Raster instance
    """

    store = None
    for ix, file_path in enumerate(file_list):

        try:
            raster = rasterio.open(file_path)
        except:
            print("Could not include file in aggregation ({0})".format(file_path))
            continue

        active = raster.read(masked=True)

        if store is None:
            store = active.copy()

        else:
            # make sure dimensions match
            if active.shape != store.shape:
                raise Exception("Dimensions of rasters do not match")

            if method == "max":
                store = np.ma.array((store, active)).max(axis=0)

                # non masked array alternatives
                # store = np.maximum.reduce([store, active])
                # store = np.vstack([store, active]).max(axis=0)

            elif method == "mean":
                if ix == 1:
                    weights = (~store.mask).astype(int)

                store = np.ma.average(np.ma.array((store, active)), axis=0, weights=[weights, (~active.mask).astype(int)])
                weights += (~active.mask).astype(int)

            elif method == "min":
                store = np.ma.array((store, active)).min(axis=0)

            elif method == "sum":
                store = np.ma.array((store, active)).sum(axis=0)

            else:
                raise Exception("Invalid method")

    store = store.filled(raster.nodata)
    return store, raster.meta
