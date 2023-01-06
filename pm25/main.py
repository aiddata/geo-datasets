# for converting MONTHLY pm data downloaded from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148055008434 
# version: multiple file download - HPC version, based off Dr. Goodman's script for converting nc file to tiff image, MONTHLY data

import os
import sys
import time
import hashlib
import datetime
import warnings
from pathlib import Path

import rasterio
import numpy as np
import pandas as pd
from affine import Affine
from netCDF4 import Dataset as NCDFDataset
from boxsdk import JWTAuth, Client

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset
                

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


# adapted from https://stackoverflow.com/a/44873382
def sha1(filename):
    h  = hashlib.sha1()
    b  = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


class PM25(Dataset):
    name = "Surface PM2.5"

    def __init__(self,
                 raw_dir: str,
                 output_dir: str,
                 box_config_path: str,
                 years: list,
                 skip_existing_downloads=True,
                 verify_existing_downloads=True):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)

        self.years = [int(y) for y in years]

        self.box_config_path = Path(box_config_path)
    
        # skip existing files while downloading?
        self.skip_existing_downloads = skip_existing_downloads

        # verify existing files' hashes while downloading?
        # (skip_existing_downloads must also be set to True)
        self.verify_existing_downloads = verify_existing_downloads

        self.overwrite = False

        self.filename_template = "V5GL02.HybridPM25.Global.{YEAR}{FIRST_MONTH}-{YEAR}{LAST_MONTH}"


    def download_items(self,
                       box_folder,
                       dst_folder,
                       skip_existing=True,
                       verify_existing=True):
        """
        Downloads the contents of a Box folder to a dst_folder

        skip_existing will skip file names that already exist
        in dst_folder verify_existing will verify the hashes
        of existing files in dst_folder, if skip_existing is
        True
        """

        logger = self.get_logger()

        os.makedirs(dst_folder, exist_ok=True)

        successful_downloads = []

        for i in box_folder.get_items():
            file_timeframe = i.name.split(".")[3].split("-")
            if len(file_timeframe) == 2:
                first_year = str(file_timeframe[0])[:4]
                second_year = str(file_timeframe[1])[:4]
                if first_year == second_year and int(first_year) in self.years:
                    
                    dst_file = os.path.join(dst_folder, i.name)

                    if skip_existing and os.path.isfile(dst_file):
                        if verify_existing:
                            if sha1(dst_file) == i.sha1:
                                logger.info(f"File already downloaded with correct hash, skipping: {dst_file}")
                                continue
                            else:
                                logger.info(f"File already exists with incorrect hash, downloading again: {dst_file}")
                        else:
                            logger.info(f"File already downloaded, skipping: {dst_file}")
                            continue
                    else:
                        logger.info(f"Downloading: {dst_file}")

                        with open(dst_file, "wb") as dst:
                            i.download_to(dst)

                    successful_downloads.append(dst_file)

                else:
                    logger.debug(f"Skipping {i.name}, year not in range for this run")
            else:
                raise Exception(f"Unable to parse file name: {i.name}")

        return successful_downloads


    def download_data(self, **kwargs):
        """
        Downloads data from the Box shared folder for this dataset.

        kwargs are passed to download_items
        """

        # load JWT authentication JSON (see README.md for how to set this up)
        auth = JWTAuth.from_settings_file(self.box_config_path)

        # create Box client
        client = Client(auth)

        # find shared folder
        shared_folder = client.get_shared_item("https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25")

        # find Global folder
        for i in shared_folder.get_items():
            if i.name == "Global":
                global_item = i

        # raise a KeyError if Global directory cannot be found
        if not global_item:
            raise KeyError("Could not find directory \"Global\" in shared Box folder")

        # find Annual and Monthly child folders
        for i in global_item.get_items():
            if i.name == "Annual":
                annual_item = i
            if i.name == "Monthly":
                monthly_item = i

        # raise a KeyError if Annual or Monthly directories cannot be found
        if not annual_item:
            raise KeyError("Could not find directory \"Global/Annual\" in shared Box folder")
        elif not monthly_item:
            raise KeyError("Could not find directory \"Global/Monthly\" in shared Box folder")

        # download Annual files
        self.download_items(annual_item, "input_data/Annual/", **kwargs)

        # download Monthly files
        self.download_items(monthly_item, "input_data/Monthly/", **kwargs)


    def convert_file(self, input_path, output_path):
        # Converts nc file to tiff file

        logger = self.get_logger()

        if output_path.exists() and not self.overwrite:
            logger.info(f"File already converted, skipping: {output_path}")
        else:
            rootgrp = NCDFDataset(input_path, "r", format="NETCDF4")

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
                "driver": "COG",
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

            logger.info(f"Exported file: {output_path}")

        return str(output_path)
    

    def build_process_list(self):

        input_path_list = []
        output_path_list = []
        
        # run annual data
        for year in year_list:
            filename = self.filename_template.format(YEAR = year, FIRST_MONTH = "01", LAST_MONTH = "12")
            input_path = self.raw_dir / "Annual" / (filename + ".nc")
            if os.path.exists(input_path):
                input_path_list.append(input_path)
                output_path = self.output_dir / "Annual" / (filename + ".tif")
                output_path_list.append(output_path)
            else:
                warnings.warn(f"No annual data found for year {year}. Skipping...")

        # run monthly data
        # TODO: find a way to set each year's month range individually so if researcher wants different months for each year can adjust
        for year in year_list:
            for i in range(1, 13):
                month = str(i).zfill(2)
                filename = self.filename_template.format(YEAR = year, FIRST_MONTH = month, LAST_MONTH = month)
                input_path = self.raw_dir / "Monthly" / (filename + ".nc")
                if os.path.exists(input_path):
                    input_path_list.append(input_path)
                    output_path = self.output_dir / "Monthly" / (filename + ".tif")
                    output_path_list.append(output_path)
                else:
                    warnings.warn(f"No monthly data found for year {year} month {month}. Skipping...")

        return list(zip(input_path_list, output_path_list))


    def main(self):

        logger = self.get_logger()

        logger.info("Downloading / Verifying Data")
        self.download_data(skip_existing=self.skip_existing_downloads, verify_existing=self.verify_existing_downloads)

        logger.info("Generating Task List")
        conv_flist = self.build_process_list()

        # create output directories
        os.makedirs(self.output_dir / "Annual", exist_ok=True)
        os.makedirs(self.output_dir / "Monthly", exist_ok=True)

        logger.info("Running Data Conversion")
        conv = self.run_tasks(self.convert_file, conv_flist)
        self.log_run(conv)


if __name__ == "__main__":
    raw_dir = Path(os.getcwd(), "input_data")
    output_dir = Path(os.getcwd(), "output_data")
    box_config_path = "box_login_config.json"

    year_list = range(1998, 2021)

    PM25(raw_dir, output_dir, box_config_path, year_list, skip_existing_downloads=True, verify_existing_downloads=False).run()
