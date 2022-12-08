"""
Download and prepare data
"""
import sys
import os
import glob
import time
import zipfile
import datetime
from pathlib import Path
from typing import Optional
from configparser import ConfigParser

import cdsapi
import rasterio
import numpy as np

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset

def raster_calc(input_path, output_path, function, **kwargs):
    """
    Calculate raster values using rasterio based on function provided
    :param input_path: input raster
    :param output_path: path to write output raster to
    :param function: function to apply to input raster values
    :param kwargs: additional meta args used to write output raster
    """
    with rasterio.open(input_path) as src:
        assert len(set(src.block_shapes)) == 1
        meta = src.meta.copy()
        meta.update(**kwargs)
        with rasterio.open(output_path, "w", **meta) as dst:
            for ji, window in src.block_windows(1):
                in_data = src.read(window=window)
                out_data = function(in_data)
                out_data = out_data.astype(meta["dtype"])
                dst.write(out_data, window=window)


def export_raster(data, path, meta, **kwargs):
    """
    Export raster array to geotiff
    """

    logger = self.get_logger()

    if not isinstance(meta, dict):
        raise ValueError("meta must be a dictionary")

    if 'dtype' in meta:
        if meta["dtype"] != data.dtype:
            logger.warning(f"Dtype specified by meta({meta['dtype']}) does not match data dtype ({data.dtype}). Adjusting data dtype to match meta.")
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
            logger.info(f"Value for `{k}` not in meta provided. Using default value ({v})")
            meta[k] = v

    # write geotif file
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(data)


class ESALandcover(Dataset):
    name = "ESA Landcover"

    def __init__(self, raw_dir, output_dir, years, overwrite=True):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.overwrite = overwrite
        self.years = [int(y) for y in years]

        self.v207_years = range(1992, 2016)
        self.v211_years = range(2016, 2021)

        self.cdsapi_client = cdsapi.Client()

        mapping = {
            0: [0],
            10: [10, 11, 12],
            20: [20],
            30: [30, 40],
            50: [50, 60, 61, 62, 70, 71, 72, 80, 81, 82, 90, 100, 160, 170],
            110: [110, 130],
            120: [120, 121, 122],
            140: [140, 150, 151, 152, 153],
            180: [180],
            190: [190],
            200: [200, 201, 202],
            210: [210],
            220: [220],
        }

        vector_mapping = {vi: k for k, v in mapping.items() for vi in v}

        self.map_func = np.vectorize(vector_mapping.get)

    def download(self, year):

        logger = self.get_logger()

        if year in self.v207_years:
            version = "v2.0.7cds"
        elif year in self.v211_years:
            version = "v2.1.1"
        else:
            version = "v2.1.1"
            logger.warning(f"Assuming that {year} is v2.1.1")

        dl_path = self.raw_dir / "compressed" / f"{year}.zip"
        print(dl_path)

        if not dl_path.exists() or self.overwrite:
            dl_meta = {
                "variable": "all",
                "format": "zip",
                "version": version,
                "year": year,
            }
            self.cdsapi_client.retrieve("satellite-land-cover", dl_meta, dl_path)

        zipfile_path = dl_path.as_posix()

        logger.info(f"Unzipping {zipfile_path}...")

        with zipfile.ZipFile(zipfile_path) as zf:
            netcdf_namelist = [i for i in zf.namelist() if i.endswith(".nc")]
            if len(netcdf_namelist) != 1:
                raise Exception(f"Multiple or no ({len(netcdf_namelist)}) net cdf files found in zip for {year}")
            output_file_path = self.raw_dir / "uncompressed" / netcdf_namelist[0]
            if (not os.path.isfile(output_file_path) or self.overwrite):
                zf.extract(netcdf_namelist[0], self.raw_dir / "uncompressed")
                logger.info(f"Unzip complete: {zipfile_path}...")
            else:
                logger.info(f"Unzip exists: {zipfile_path}...")

        return output_file_path


    def process(self, input_path, output_path):
        logger = self.get_logger()
        logger.info(f"Processing: {input_path}")
        kwargs = {"driver": "GTiff", "compress": "LZW"}
        netcdf_path = f"netcdf:{input_path}:lccs_class"
        raster_calc(netcdf_path, output_path, self.map_func, **kwargs)


    def main(self):

        os.makedirs(self.raw_dir / "compressed", exist_ok=True)
        os.makedirs(self.raw_dir / "uncompressed", exist_ok=True)

        # Download data
        download = self.run_tasks(self.download, [[y] for y in self.years])
        self.log_run(download)

        os.makedirs(self.output_dir, exist_ok=True)

        # Process data
        process_inputs = zip(download.results(), [self.output_dir / f"esa_lc_{year}.tif" for year in self.years])
        process = self.run_tasks(self.process, process_inputs)
        self.log_run(process)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["Config"]["raw_dir"]),
        "output_dir": Path(config["Config"]["output_dir"]),
        "years": [int(y) for y in config["Config"]["years"].split(", ")],
        "overwrite": config["Config"].getboolean("overwrite"),
        "backend": config["Config"]["backend"],
        "task_runner": config["Config"]["task_runner"],
        "run_parallel": config["Config"].getboolean("run_parallel"),
        "max_workers": int(config["Config"]["max_workers"]),
        "log_dir": Path(config["Config"]["raw_dir"]) / "logs"
    }

if __name__ == "__main__":

    config_dict = get_config_dict()

    class_instance = ESALandcover(raw_dir=config_dict["raw_dir"], output_dir=config_dict["output_dir"], years=config_dict["years"], overwrite=config_dict["overwrite"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=config_dict["log_dir"])