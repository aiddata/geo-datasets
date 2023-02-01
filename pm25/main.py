# for converting MONTHLY pm data downloaded from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148055008434
# version: multiple file download - HPC version, based off Dr. Goodman's script for converting nc file to tiff image, MONTHLY data

import os
import sys
import hashlib
import warnings
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser

import rasterio
import numpy as np
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


def create_box_client(box_config_path):
    """
    Creates a Box client using the provided JWT authentication JSON.
    """

    # load JWT authentication JSON (see README.md for how to set this up)
    auth = JWTAuth.from_settings_file(box_config_path)

    # create Box client
    client = Client(auth)

    return client


class PM25(Dataset):
    name = "Surface PM2.5"

    def __init__(self,
                 raw_dir: str,
                 output_dir: str,
                 box_config_path: str,
                 version: str,
                 years: list,
                 skip_existing_downloads=True,
                 verify_existing_downloads=True,
                 overwrite_processing=False,):

        self.version = version

        self.raw_dir = Path(raw_dir) / self.version
        self.output_dir = Path(output_dir) / self.version

        self.years = [int(y) for y in years]

        self.box_config_path = Path(box_config_path)

        # skip existing files while downloading?
        self.skip_existing_downloads = skip_existing_downloads

        # verify existing files' hashes while downloading?
        # (skip_existing_downloads must also be set to True)
        self.verify_existing_downloads = verify_existing_downloads

        self.overwrite_processing = overwrite_processing

        self.filename_template =  self.version + ".HybridPM25.Global.{YEAR}{FIRST_MONTH}-{YEAR}{LAST_MONTH}"


    def build_file_download_list(self):
        """
        Generates a task list of downloads from the Box shared folder for this dataset.
        """
        logger = self.get_logger()

        # create Box client
        self.client = create_box_client(self.box_config_path)

        # find shared folder
        shared_folder = self.client.get_shared_item(f"https://wustl.app.box.com/v/ACAG-{self.version}-GWRPM25")

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


        annual_item_list = [(i, self.raw_dir / "Global" / "Annual" / i.name) for i in annual_item.get_items()]
        monthly_item_list = [(i, self.raw_dir / "Global" / "Monthly" / i.name) for i in monthly_item.get_items()]

        tmp_download_item_list = annual_item_list + monthly_item_list

        download_item_list = []

        for item, dst_file in tmp_download_item_list:

            file_timeframe = item.name.split(".")[3].split("-")
            if len(file_timeframe) == 2:

                first_year = str(file_timeframe[0])[:4]
                second_year = str(file_timeframe[1])[:4]

                if first_year == second_year and int(first_year) in self.years:

                    if self.skip_existing_downloads and os.path.isfile(dst_file):
                        if self.verify_existing_downloads:
                            logger.info(f"File exists but adding to download list for verification: {dst_file}")
                            download_item_list.append((item.id, dst_file))
                        else:
                            logger.info(f"File already downloaded, skipping: {dst_file}")
                    else:
                        logger.info(f"Adding to download list: {dst_file}")
                        download_item_list.append((item.id, dst_file))

                else:
                    logger.debug(f"Skipping {item.name}, year not in range for this run")
            else:
                raise Exception(f"Unable to parse file name: {item.name}")

        del self.client

        return download_item_list


    def download_file(self, item_id, dst_file):

        logger = self.get_logger()

        client = create_box_client(self.box_config_path)
        item = client.file(item_id).get()

        run_download = True
        if self.skip_existing_downloads and os.path.isfile(dst_file):
            if sha1(dst_file) == item.sha1:
                logger.info(f"File already downloaded with correct hash, skipping: {dst_file}")
                run_download = False
            else:
                logger.info(f"File already exists with incorrect hash {dst_file}")

        if run_download:
            logger.info(f"Downloading: {dst_file}")
            with open(dst_file, "wb") as dst:
                item.download_to(dst)

        del client


    def convert_file(self, input_path, output_path):
        # Converts nc file to tiff file

        logger = self.get_logger()
        logger.info(f"Converting file: {input_path}")

        if output_path.exists() and not self.overwrite_processing:
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

        task_list = []

        # run annual data
        for year in self.years:
            filename = self.filename_template.format(YEAR = year, FIRST_MONTH = "01", LAST_MONTH = "12")
            input_path = self.raw_dir / "Global" / "Annual" / (filename + ".nc")
            if input_path.exists():
                output_path = self.output_dir / "Global" / "Annual" / (filename + ".tif")
                task_list.append((input_path, output_path))
            else:
                warnings.warn(f"No annual data found for year {year}. Skipping...")

        # run monthly data
        # TODO: find a way to set each year's month range individually so if researcher wants different months for each year can adjust
        for year in self.years:
            for i in range(1, 13):
                month = str(i).zfill(2)
                filename = self.filename_template.format(YEAR = year, FIRST_MONTH = month, LAST_MONTH = month)
                input_path = self.raw_dir / "Global" / "Monthly" / (filename + ".nc")
                if input_path.exists():
                    output_path = self.output_dir / "Global" / "Monthly" / (filename + ".tif")
                    task_list.append((input_path, output_path))
                else:
                    warnings.warn(f"No monthly data found for year {year} month {month}. Skipping...")

        return task_list


    def main(self):

        logger = self.get_logger()


        logger.info("Building initial download list")
        dl_file_list = self.build_file_download_list()

        (self.raw_dir / "Global" / "Annual").mkdir(parents=True, exist_ok=True)
        (self.raw_dir / "Global" / "Monthly").mkdir(parents=True, exist_ok=True)

        logger.info("Downloading Data")
        dl = self.run_tasks(self.download_file, dl_file_list)#, force_serial=True)
        self.log_run(dl)


        logger.info("Generating Task List")
        conv_flist = self.build_process_list()
        logger.info(conv_flist)

        # create output directories
        (self.output_dir / "Global" / "Annual").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "Global" / "Monthly").mkdir(parents=True, exist_ok=True)

        logger.info("Running Data Conversion")
        conv = self.run_tasks(self.convert_file, conv_flist)
        self.log_run(conv)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "version": config["main"]["version"],
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "box_config_path": Path(config["main"]["box_config_path"]),
        "skip_existing_downloads": config["main"].getboolean("skip_existing_downloads"),
        "verify_existing_downloads": config["main"].getboolean("verify_existing_downloads"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs",
    }


if __name__ == "__main__":

    config_dict = get_config_dict()

    log_dir = config_dict["log_dir"]
    timestamp = datetime.today()
    time_format_str: str="%Y_%m_%d_%H_%M"
    time_str = timestamp.strftime(time_format_str)
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)


    class_instance = PM25(config_dict["raw_dir"], config_dict["output_dir"], config_dict["box_config_dict_path"], config_dict["version"], config_dict["years"], config_dict["skip_existing_downloads"], config_dict["verify_existing_downloads"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
