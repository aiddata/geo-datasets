import os
import copy
import time
import datetime
import warnings
import requests
import hashlib


import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import h5py
import rasterio
from scipy.interpolate import griddata
from affine import Affine


def download_file(url, local_filename):
    """Download a file from url to local_filename

    Downloads in chunks
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                f.write(chunk)


def get_md5sum_from_xml_url(url, field):
    """Read the md5sum from an xml file

    - XML file provided as url
    - XML field containing md5sum must be provided
    """
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    md5sum = soup.find(field).text
    return md5sum


def calc_md5sum(path):
    """Calculate the md5sum for a file
    """
    with open(path, "rb") as f:
        md5sum = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            md5sum.update(chunk)
            chunk = f.read(8192)
        return md5sum.hexdigest()


def find_files(url, ext=''):
    """Find all files on a webpage

    Option matching on string at end of links founds

    Returns list of complete (absolute) links
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    urllist = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return urllist


def file_exists(path):
    return os.path.isfile(path)


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def concat_data(flist, out_path):
    """concat daily data csv to monthly data csv
    """
    df_list = [read_csv(f) for f in flist]
    out = pd.concat(df_list, axis=0, ignore_index=True)
    out.to_csv(out_path, index=False, encoding='utf-8')


def round_to(value, interval):
    """round value to nearest interval of a decimal value
    e.g., every 0.25
    """
    if interval > 1:
        raise ValueError("Must provide float less than (or equal to) 1 indicating interval to round to")
    return round(value * (1/interval)) * interval


def lonlat(lon, lat, dlen):
    """create unique id string combining latitude and longitude
    """
    str_lon = format(lon, '0.{}f'.format(dlen))
    str_lat = format(lat, '0.{}f'.format(dlen))
    lon_lat = "{}_{}".format(str_lon, str_lat)
    return lon_lat


def agg_to_grid(input_path, output_path, rnd_interval=0.1):
    """aggregate coordinates to regular grid points
    """
    decimal_places = len(str(rnd_interval).split(".")[1])
    df = read_csv(input_path)
    df = df.loc[df["xco2_quality_flag"] == 0].copy(deep=True)
    df["lon"] = df["lon"].apply(lambda z: round_to(z, rnd_interval))
    df["lat"] = df["lat"].apply(lambda z: round_to(z, rnd_interval))
    df["lonlat"] = df.apply(lambda z: lonlat(z["lon"], z["lat"], decimal_places), axis=1)
    agg_ops = {
        'xco2': "mean",
        'xco2_quality_flag': "count",
        'lon': "first",
        'lat': 'first'
    }
    agg_df = df.groupby('lonlat', as_index=False).agg(agg_ops)
    agg_df.columns = [i.replace("xco2_quality_flag", "count") for i in agg_df.columns]
    agg_df.to_csv(output_path, index=False, encoding='utf-8')


def interpolate(input_path, output_path, rnd_interval=0.1, interp_method="linear"):
    """interpolate
    https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.griddata.html#scipy.interpolate.griddata
    https://earthscience.stackexchange.com/questions/12057/how-to-interpolate-scattered-data-to-a-regular-grid-in-python
    """
    data = read_csv(input_path)
    # data coordinates and values
    x = data["lon"]
    y = data["lat"]
    z = data["xco2"]
    # target grid to interpolate to
    xi = np.arange(-180.0, 180.0+rnd_interval, rnd_interval)
    yi = np.arange(90.0, -90.0-rnd_interval, -rnd_interval)
    xi, yi = np.meshgrid(xi,yi)
    # interpolate
    zi = griddata((x, y), z, (xi, yi), method=interp_method)
    # prepare raster
    transform = Affine(rnd_interval, 0, -180.0,
                    0, -rnd_interval, 90.0)
    meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'dtype': str(zi.dtype),
        'transform': transform,
        'driver': 'GTiff',
        'height': zi.shape[0],
        'width': zi.shape[1]
    }
    raster_out = np.array([zi])
    with rasterio.open(output_path, "w", **meta) as dst:
        dst.write(raster_out)


def read_csv(path):
    df = pd.read_csv(
        path, quotechar='\"',
        na_values='', keep_default_na=False,
        encoding='utf-8')
    return df


def convert_daily(input_path, output_path):
    """convert daily nc4 files to csv
    """
    print("Converting {}".format(output_path))
    with h5py.File(input_path, 'r') as hdf_data:
        lon = list(hdf_data["longitude"])
        lat = list(hdf_data["latitude"])
        xco2 = list(hdf_data["xco2"])
        xco2_quality_flag = list(hdf_data["xco2_quality_flag"])
    point_list = list(zip(lon, lat, xco2, xco2_quality_flag))
    df = pd.DataFrame(point_list, columns=["lon", "lat", "xco2", "xco2_quality_flag"])
    df.to_csv(output_path, index=False, encoding='utf-8')


def concat_month(flist, out_path):
    print("Concat yearmonth {}".format(out_path))
    concat_data(flist, out_path)


def concat_year(flist, out_path):
    print("Concat year {}".format(out_path))
    concat_data(flist, out_path)


def agg_to_grid_month(input_path, output_path):
    print("Agg {}".format(output_path))
    agg_to_grid(input_path, output_path)


def agg_to_grid_year(input_path, output_path):
    print("Agg {}".format(output_path))
    agg_to_grid(input_path, output_path)


def interpolate_month(input_path, output_path):
    print("Interpolating {}".format(output_path))
    interpolate(input_path, output_path)


def interpolate_year(input_path, output_path):
    print("Interpolating {}".format(output_path))
    interpolate(input_path, output_path)


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
