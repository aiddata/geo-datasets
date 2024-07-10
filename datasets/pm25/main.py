# for converting MONTHLY pm data downloaded from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148055008434
# version: multiple file download - HPC version, based off Dr. Goodman's script for converting nc file to tiff image, MONTHLY data

import hashlib
import os
import warnings
from pathlib import Path
from typing import List

import numpy as np
import rasterio
from affine import Affine
from boxsdk import Client, JWTAuth
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from netCDF4 import Dataset as NCDFDataset
from pydantic import BaseModel


def export_raster(data, path, meta, **kwargs):
    """
    Export raster array to geotiff
    """

    if not isinstance(meta, dict):
        raise ValueError("meta must be a dictionary")

    if "dtype" in meta:
        if meta["dtype"] != data.dtype:
            warnings.warn(
                f"Dtype specified by meta({meta['dtype']}) does not match data dtype ({data.dtype}). Adjusting data dtype to match meta."
            )
        data = data.astype(meta["dtype"])
    else:
        meta["dtype"] = data.dtype

    default_meta = {
        "count": 1,
        "crs": {"init": "epsg:4326"},
        "driver": "GTiff",
        "compress": "lzw",
        "nodata": -9999,
    }

    for k, v in default_meta.items():
        if k not in meta:
            if "quiet" not in kwargs or kwargs["quiet"] == False:
                print(
                    f"Value for `{k}` not in meta provided. Using default value ({v})"
                )
            meta[k] = v

    # write geotif file
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(data)


# adapted from https://stackoverflow.com/a/44873382
def sha1(filename):
    h = hashlib.sha1()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


class BoxAppAuth(BaseModel):
    publicKeyID: str
    privateKey: str
    passphrase: str


class BoxAppSettings(BaseModel):
    clientID: str
    clientSecret: str
    appAuth: BoxAppAuth
    enterpriseID: str


class BoxConfig(BaseModel):
    boxAppSettings: BoxAppSettings


class PM25Configuration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    box_config: BoxConfig
    version: str
    years: List[int]
    overwrite_downloads: bool
    verify_existing_downloads: bool
    overwrite_processing: bool


class PM25(Dataset):
    name = "Surface PM2.5"

    def __init__(self, config: PM25Configuration):

        self.version = config.version

        self.raw_dir = Path(config.raw_dir) / self.version
        self.output_dir = Path(config.output_dir) / self.version

        self.years = config.years

        self.box_config = config.box_config

        # skip existing files while downloading?
        self.overwrite_downloads = config.overwrite_downloads

        # verify existing files' hashes while downloading?
        self.verify_existing_downloads = config.verify_existing_downloads

        self.overwrite_processing = config.overwrite_processing

        self.filename_template = (
            self.version + ".HybridPM25.Global.{YEAR}{FIRST_MONTH}-{YEAR}{LAST_MONTH}"
        )

    def create_box_client(self):
        """
        Creates a Box client using the provided JWT authentication JSON.
        """

        logger = self.get_logger()

        # load JWT authentication JSON (see README.md for how to set this up)
        auth = JWTAuth.from_settings_dictionary(self.box_config)

        # create Box client
        client = Client(auth)

        return client

    def build_file_download_list(self):
        """
        Generates a task list of downloads from the Box shared folder for this dataset.
        """
        logger = self.get_logger()

        # create Box client
        self.client = self.create_box_client()

        # find shared folder
        shared_folder = self.client.get_shared_item(
            f"https://wustl.app.box.com/v/ACAG-{self.version}-GWRPM25"
        )

        # find Global folder
        for i in shared_folder.get_items():
            if i.name == "Global":
                global_item = i

        # raise a KeyError if Global directory cannot be found
        if not global_item:
            raise KeyError('Could not find directory "Global" in shared Box folder')

        # find Annual and Monthly child folders
        for i in global_item.get_items():
            if i.name == "Annual":
                annual_item = i
            if i.name == "Monthly":
                monthly_item = i

        # raise a KeyError if Annual or Monthly directories cannot be found
        if not annual_item:
            raise KeyError(
                'Could not find directory "Global/Annual" in shared Box folder'
            )
        elif not monthly_item:
            raise KeyError(
                'Could not find directory "Global/Monthly" in shared Box folder'
            )

        annual_item_list = [
            (i, self.raw_dir / "Global" / "Annual" / i.name)
            for i in annual_item.get_items()
        ]
        monthly_item_list = [
            (i, self.raw_dir / "Global" / "Monthly" / i.name)
            for i in monthly_item.get_items()
        ]

        tmp_download_item_list = annual_item_list + monthly_item_list

        download_item_list = []

        for item, dst_file in tmp_download_item_list:

            file_timeframe = item.name.split(".")[3].split("-")
            if len(file_timeframe) == 2:

                first_year = str(file_timeframe[0])[:4]
                second_year = str(file_timeframe[1])[:4]

                if first_year == second_year and int(first_year) in self.years:

                    if not os.path.isfile(dst_file) or self.overwrite_downloads:
                        logger.info(f"Adding to download list: {dst_file}")
                        download_item_list.append((item.id, dst_file))

                    elif os.path.isfile(dst_file) and self.verify_existing_downloads:
                        logger.info(
                            f"File exists but adding to download list for verification: {dst_file}"
                        )
                        download_item_list.append((item.id, dst_file))
                    else:
                        logger.info(f"File already downloaded, skipping: {dst_file}")

                else:
                    logger.debug(
                        f"Skipping {item.name}, year not in range for this run"
                    )
            else:
                raise Exception(f"Unable to parse file name: {item.name}")

        del self.client

        return download_item_list

    def get_box_item(self, id: str):
        client = self.create_box_client()
        box_folder_url = f"https://wustl.app.box.com/v/ACAG-{self.version}-GWRPM25"

        for i in client.get_shared_item(box_folder_url).get_items():
            if i.name == "Global":
                for j in i.get_items():
                    if j.name in ["Annual", "Monthly"]:
                        for k in j.get_items():
                            if k.id == id:
                                return k

        raise KeyError(f"Could not find file id: {id}")

    def download_file(self, item_id, dst_file):

        logger = self.get_logger()
        # logger.info(f"DEBUG AA: {item} ---- {dst_file}")

        try:
            logger.info(f"Retrieving box file item for {item_id} - {dst_file}")
            item = self.get_box_item(item_id)
        except:
            logger.error(f"Unable to find file id ({item_id}) for {dst_file}")
            raise

        run_download = True
        if os.path.isfile(dst_file) and self.verify_existing_downloads:
            if sha1(dst_file) == item.sha1:
                logger.info(
                    f"File already downloaded with correct hash, skipping: {dst_file}"
                )
                run_download = False
            else:
                logger.info(f"File already exists with incorrect hash {dst_file}")

        if run_download:
            logger.info(f"Downloading: {dst_file}")
            with open(dst_file, "wb") as dst:
                item.download_to(dst)

        # del client

        # logger.info(f"DEBUG BB: {item} ---- {dst_file}")

    def build_process_list(self):

        task_list = []

        # run annual data
        for year in self.years:
            filename = self.filename_template.format(
                YEAR=year, FIRST_MONTH="01", LAST_MONTH="12"
            )
            input_path = self.raw_dir / "Global" / "Annual" / (filename + ".nc")
            if input_path.exists():
                output_path = (
                    self.output_dir / "Global" / "Annual" / (filename + ".tif")
                )
                task_list.append((input_path, output_path))
            else:
                warnings.warn(f"No annual data found for year {year}. Skipping...")

        # run monthly data
        # TODO: find a way to set each year's month range individually so if researcher wants different months for each year can adjust
        for year in self.years:
            for i in range(1, 13):
                month = str(i).zfill(2)
                filename = self.filename_template.format(
                    YEAR=year, FIRST_MONTH=month, LAST_MONTH=month
                )
                input_path = self.raw_dir / "Global" / "Monthly" / (filename + ".nc")
                if input_path.exists():
                    output_path = (
                        self.output_dir / "Global" / "Monthly" / (filename + ".tif")
                    )
                    task_list.append((input_path, output_path))
                else:
                    warnings.warn(
                        f"No monthly data found for year {year} month {month}. Skipping..."
                    )

        return task_list

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
                "transform": Affine(lon_res, 0.0, lon_min, 0.0, -lat_res, lat_max),
            }

            export_raster(np.array([data.data]), output_path, meta)

            logger.info(f"Exported file: {output_path}")

        return str(output_path)

    def main(self):

        logger = self.get_logger()

        logger.info("Building initial download list")
        dl_file_list = self.build_file_download_list()

        (self.raw_dir / "Global" / "Annual").mkdir(parents=True, exist_ok=True)
        (self.raw_dir / "Global" / "Monthly").mkdir(parents=True, exist_ok=True)

        if len(dl_file_list) > 0:
            logger.info("Downloading Data")
            dl = self.run_tasks(self.download_file, dl_file_list, force_sequential=True)
            self.log_run(dl)
        else:
            logger.info("Skipping download, no files queued for download")

        logger.info("Generating Task List")
        conv_flist = self.build_process_list()

        # create output directories
        (self.output_dir / "Global" / "Annual").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "Global" / "Monthly").mkdir(parents=True, exist_ok=True)

        logger.info("Running Data Conversion")
        conv = self.run_tasks(self.convert_file, conv_flist, force_sequential=True)
        self.log_run(conv)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def pm25(config: PM25Configuration):
        PM25(config).run(config.run)


if __name__ == "__main__":
    config = get_config(PM25Configuration)
    PM25(config).run(config.run)
