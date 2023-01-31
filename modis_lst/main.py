import os
import sys
import requests
import shutil
from pathlib import Path
from urllib.parse import urlparse
from configparser import ConfigParser

import numpy as np
from affine import Affine

import warnings
import rasterio
from bs4 import BeautifulSoup
from pyhdf.SD import SD, SDC

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


def listFD(url, ext=''):
    """Find all links in a webpage

    Option matching on string at end of links founds

    Returns list of complete (absolute) links
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    urllist = [url + "/" + node.get('href').strip("/") for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return urllist


class SessionWithHeaderRedirection(requests.Session):
    """
    overriding requests.Session.rebuild_auth to mantain headers when redirected
    from: https://wiki.earthdata.nasa.gov/display/EL/How+To+Access+Data+With+Python
    """

    AUTH_HOST = 'urs.earthdata.nasa.gov'
    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)
    def rebuild_auth(self, prepared_request, response):
        """
        Overrides from the library to keep headers when redirected to or from
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


def export_raster(data, path, meta, **kwargs):
    """
    Export raster array to geotiff
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
        'driver': 'COG',
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
    """
    Aggregate multiple rasters

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


class MODISLandSurfaceTemp(Dataset):
    name = "MODIS Land Surface Temperatures"

    def __init__(self, process_dir, raw_dir, output_dir, username, password, years, overwrite_download, overwrite_processing):
        self.username = username
        self.password = password

        self.years = [str(y) for y in years]

        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing

        self.root_url = "https://e4ftl01.cr.usgs.gov"
        self.data_url = os.path.join(self.root_url, "MOLT/MOD11C3.061")

        self.process_dir = Path(process_dir)
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)

        self.process_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.method = "mean"



    def test_connection(self):
        logger = self.get_logger()
        logger.info("Testing connection...")

        test_request = requests.get(self.data_url)
        test_request.raise_for_status()


    def download_file(self, url, tmp_file, dst_file, identifier):
        """
        download individual file using session created

        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """

        logger = self.get_logger()

        if dst_file.exists() and not self.overwrite_download:
            logger.info(f"File already exists: {dst_file}. Skipping...")
        else:
            Path(tmp_file).parent.mkdir(parents=True, exist_ok=True)

            # create session with the user credentials that will be used to authenticate access to the data
            # Note: session can be serialized but because we are streaming the files it cannot
            session = SessionWithHeaderRedirection(self.username, self.password)
            # release the connection pool until one file is completed. Instead we create a new
            # session for each process to use on its own.

            with session.get(url, stream=True) as r:
                r.raise_for_status()
                with open(tmp_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)

            logger.info(f"Downloaded to tmp: {url} > {tmp_file}")
            shutil.copyfile(tmp_file, dst_file)
            logger.info(f"Copied to dst: {tmp_file} > {dst_file}")


    def build_download_list(self):

        logger = self.get_logger()

        logger.info("Preparing data download")

        # Note: calling listFD outside of the main block (on every process)
        #       caused some connection issues. Did not debug issue further
        #       as there is no need for this to be outside of main.
        """
        Filters links based on:
        1) dirname within link matches url being searched (i.e., not a link to an external page, only subdirectories of current url)
        2) basename matches YYYY.MM.DD format
        3) YYYY in basename matches a year in year_list
        """

        logger = self.get_logger()

        flist = []
        missing_files_count = 0

        url_list = listFD(self.data_url)

        for url in url_list:
            u = urlparse(url)
            if u.netloc == urlparse(self.data_url).netloc:
                p = Path(u.path)
                if len(p.name.split(".")) == 3:
                    if p.name.split(".")[0] in self.years:

                        # get full url for each hdf file
                        hdf_url_list = listFD(url, 'hdf')
                        if len(hdf_url_list) == 0:
                            hdf_url = "Error"
                            missing_files_count += 1
                        else:
                            hdf_url = hdf_url_list[0]

                        # get temporal string from url parent directory
                        # convert from YYYY.MM.DD to YYYYMM
                        temporal = "".join(p.name.split(".")[0:2])

                        # use basename from url to create local filename
                        hdf_url_name = Path(urlparse(hdf_url).path).name
                        tmp_path = self.process_dir / (f"{temporal}_{hdf_url_name}")
                        dst_path = self.raw_dir / (f"{temporal}_{hdf_url_name}")

                        flist.append((hdf_url, tmp_path, dst_path, temporal))

        # confirm HDF url was found for each temporal directory
        missing_files_msg = f"{missing_files_count} missing HDF files"
        if missing_files_count > 0:
            logger.warning(missing_files_msg)
        else:
            logger.info(missing_files_msg)

        return flist


    def process_hdf(self, input_path, layer, tmp_path, output_path, identifier):

        logger = self.get_logger()

        if self.overwrite_processing or not os.path.isfile(output_path):
            # read HDF data files
            file = SD(input_path, SDC.READ)
            img = file.select(layer)
            data = img.get() * img.attributes()["scale_factor"]

            # define the affine transformation
            #   5600m or 0.05 degree resolution
            #   global coverage
            transform = Affine(0.05,     0, -180,
                                  0, -0.05,   90)
            meta = {"transform": transform, "nodata": 0, "height": data.shape[0], "width": data.shape[1]}
            # need to wrap data in array so it is 3-dimensions to account for raster band
            export_raster(np.array([data]), tmp_path, meta, quiet=True)

            logger.info(f"Processed to tmp: {input_path} > {tmp_path}")
            shutil.copyfile(tmp_path, output_path)
            logger.info(f"Copied to dst: {tmp_path} > {output_path}")

        else:
            logger.info(f"{output_path} already exists, skipping...")


    def build_process_list(self):

        flist = []
        output_path_list = []

        for l_time, c_time in [("day", "Day"), ("night", "Night")]:
            for p in self.raw_dir.iterdir():
                if p.suffix == ".hdf":
                    temporal = p.name.split("_")[0]
                    output_path = self.output_dir / "monthly" / l_time / f"modis_lst_{l_time}_cmg_{temporal}.tif"
                    tmp_path = self.process_dir / f"modis_lst_{l_time}_cmg_{temporal}.tif"

                    output_path_list.append(output_path)
                    layer = f"LST_{c_time}_CMG"

                    flist.append((p.as_posix(), layer, tmp_path.as_posix(), output_path.as_posix(), temporal))

        for i in set(output_path_list):
            i.parent.mkdir(parents=True, exist_ok=True)

        return flist


    def run_yearly_data(self, year, year_files, method, tmp_path, out_path):
        logger = self.get_logger()

        if not os.path.isfile(out_path) or self.overwrite_processing:
            data, meta = aggregate_rasters(file_list=year_files, method=method)
            export_raster(data, tmp_path, meta)

            logger.info(f"Processed to tmp: {year}_{method} > {tmp_path}")
            shutil.copyfile(tmp_path, out_path)
            logger.info(f"Copied to dst: {tmp_path} > {out_path}")


    def build_aggregation_list(self):

        src_dir = self.output_dir / "monthly"

        dst_dir = self.output_dir / "annual"
        dst_dir.mkdir(parents=True, exist_ok=True)

        flist = []
        output_dir_list = []
        data_class_list = ["day", "night"]

        for data_class in data_class_list:
            month_files = [c for c in (src_dir / data_class).iterdir() if c.suffix == ".tif"]
            year_months = {}

            for mfile in month_files:
                # year associated with month
                myear = mfile.name.split("_")[-1][:4]
                if myear not in year_months:
                    year_months[myear] = list()
                year_months[myear].append(mfile.as_posix())

            for year_group, month_paths in year_months.items():
                output_path = dst_dir / data_class / self.method / f"modis_lst_{data_class}_cmg_{year_group}.tif"
                tmp_path = self.process_dir / f"{self.method}_modis_lst_{data_class}_cmg_{year_group}.tif"

                output_dir_list.append(output_path)

                flist.append((year_group, month_paths, self.method, tmp_path, output_path.as_posix()))

        for i in set(output_dir_list):
            os.makedirs(i.parent, exist_ok=True)

        return flist


    def main(self):

        # Test Connection
        self.test_connection()

        # Download
        download_list = self.build_download_list()
        download = self.run_tasks(self.download_file, download_list)
        self.log_run(download)

        # Process
        process_list = self.build_process_list()
        process = self.run_tasks(self.process_hdf, process_list)
        self.log_run(process)

        # Aggregate
        data_to_agg = self.build_aggregation_list()
        agg = self.run_tasks(self.run_yearly_data, data_to_agg)
        self.log_run(agg)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "process_dir": Path(config["main"]["process_dir"]),
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "username": config["main"]["username"],
        "password": config["main"]["password"],
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs",
    }


if __name__ == "__main__":

    config_dict = get_config_dict()

    class_instance = MODISLandSurfaceTemp(config_dict["process_dir"], config_dict["raw_dir"], config_dict["output_dir"], config_dict["username"], config_dict["password"], config_dict["years"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=config_dict["log_dir"])
