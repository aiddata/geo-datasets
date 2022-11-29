import os
import time
import datetime
import requests
from pathlib import Path
from urllib.parse import urlparse
from configparser import ConfigParser

import numpy as np
from prefect import flow
from affine import Affine

from dataset import Dataset

from utility import listFD, get_hdf_url, SessionWithHeaderRedirection, export_raster, load_hdf, aggregate_rasters


class MODISLandSurfaceTemp(Dataset):
    name = "MODIS Land Surface Temperatures"

    def __init__(self, raw_dir, output_dir, username, password, years):
        self.username = username
        self.password = password

        self.years = [str(y) for y in years]
        
        self.overwrite = False

        self.root_url = "https://e4ftl01.cr.usgs.gov"
        self.data_url = os.path.join(self.root_url, "MOLT/MOD11C3.006")

        self.raw_dir = Path(raw_dir)
        os.makedirs(self.raw_dir / "monthly" / "day", exist_ok=True)
        os.makedirs(self.raw_dir / "monthly" / "night", exist_ok=True)
        self.output_dir = Path(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

        self.method = "mean"

    def test_connection(self):
        logger = self.get_logger()
        logger.info("Testing connection...")

        test_request = requests.get(self.data_url)
        test_request.raise_for_status()
        

    def download_file(self, url, local_filename, identifier):
        """download individual file using session created

        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """

        logger = self.get_logger()

        if os.path.isfile(local_filename) and not self.overwrite:
            logger.info(f"File exists: {local_filename}. Skipping...")
        else:
            # create session with the user credentials that will be used to authenticate access to the data
            # Note: session can be serialized but because we are streaming the files it cannot
            session = SessionWithHeaderRedirection(self.username, self.password)
            # release the connection pool until one file is completed. Instead we create a new
            # session for each process to use on its own.

            with session.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)
            logger.info(f"Wrote file: {local_filename}")

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
                        hdf_url = get_hdf_url(url)
                        if hdf_url == "Error":
                            missing_files_count += 1

                        # get temporal string from url parent directory
                        # convert from YYYY.MM.DD to YYYYMM
                        temporal = "".join(p.name.split(".")[0:2])

                        # use basename from url to create local filename
                        hdf_url_name = Path(urlparse(hdf_url).path).name
                        output = self.raw_dir / (f"{temporal}_{hdf_url_name}")
                        
                        if self.overwrite or not output.exists():
                            flist.append((hdf_url, output.as_posix(), temporal))
                        else:
                            logger.info(f"{output.as_posix()} already downloaded, skipping...")

        # confirm HDF url was found for each temporal directory
        missing_files_msg = f"{missing_files_count} missing HDF files"
        if missing_files_count > 0:
            logger.warning(missing_files_msg)
        else:
            logger.info(missing_files_msg)

        return flist


    def process_hdf(self, input_path, layer, output_path, identifier):

        logger = self.get_logger()
        
        if self.overwrite or not os.path.isfile(output_path):
            data = load_hdf(input_path, layer)
            # define the affine transformation
            #   5600m or 0.05 degree resolution
            #   global coverage
            transform = Affine(0.05,     0, -180,
                                  0, -0.05,   90)
            meta = {"transform": transform, "nodata": 0, "height": data.shape[0], "width": data.shape[1]}
            # need to wrap data in array so it is 3-dimensions to account for raster band
            export_raster(np.array([data]), output_path, meta, quiet=True)
        else:
            logger.info(f"{output_path} already exists, skipping...")


    def build_process_list(self):

        flist = []
        output_path_list = []

        for time, Time in [("day", "Day"), ("night", "Night")]:
            for p in self.raw_dir.iterdir():
                if p.suffix == ".hdf":
                    temporal = p.name.split("_")[0]
                    output_path = self.output_dir / "monthly" / time / f"modis_lst_day_cmg_{temporal}.tif"
                    output_path_list.append(output_path)
                    layer = f"LST_{Time}_CMG"

                    flist.append((p.as_posix(), layer, output_path.as_posix(), temporal))

        for i in set(output_path_list):
            os.makedirs(i.parent, exist_ok=True)

        return flist

    def run_yearly_data(self, year, year_files, method, out_path):
        data, meta = aggregate_rasters(file_list=year_files, method=method)
        export_raster(data, out_path, meta)


    def build_aggregation_list(self):

        src_dir = self.output_dir / "monthly"

        dst_dir = self.output_dir / "annual"
        os.makedirs(dst_dir, exist_ok = True)

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
                output_dir_list.append(output_path)

                flist.append((year_group, month_paths, self.method, output_path.as_posix()))

        for i in set(output_dir_list):
            os.makedirs(i.parent, exist_ok=True)

        return flist


    def main(self):

        # Test Connection
        self.test_connection()


        # Download
        download_list = self.build_download_list()
        download = self.run_tasks(self.download_file, download_list, allow_futures=False)
        self.log_run(download)


        # Process
        process_list = self.build_process_list()
        process = self.run_tasks(self.process_hdf, process_list, allow_futures=False)
        self.log_run(process)


        # Aggregate
        data_to_agg = self.build_aggregation_list()
        agg = self.run_tasks(self.run_yearly_data, data_to_agg)
        self.log_run(agg)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["Config"]["raw_dir"]),
        "output_dir": Path(config["Config"]["output_dir"]),
        "username": config["Config"]["username"],
        "password": config["Config"]["password"],
        "years": [int(y) for y in config["Config"]["years"].split(", ")],
    }


if __name__ == "__main__":
    MODISLandSurfaceTemp(**get_config_dict()).run()
