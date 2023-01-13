"""
Download and prepare data
"""
import sys
import os
import zipfile
import shutil
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser

import cdsapi
import rasterio
import numpy as np

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class ESALandcover(Dataset):
    name = "ESA Landcover"

    def __init__(self, raw_dir, process_dir, output_dir, years, overwrite_download=False, overwrite_processing=False):

        self.raw_dir = Path(raw_dir)
        self.process_dir = Path(process_dir)
        self.output_dir = Path(output_dir)
        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing
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

        if not dl_path.exists() or self.overwrite_download:
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
            if (not os.path.isfile(output_file_path) or self.overwrite_download):
                zf.extract(netcdf_namelist[0], self.raw_dir / "uncompressed")
                logger.info(f"Unzip complete: {zipfile_path}...")
            else:
                logger.info(f"Unzip exists: {zipfile_path}...")

        return output_file_path


    def process(self, input_path, output_path):
        logger = self.get_logger()

        if self.overwrite_download and not self.overwrite_processing:
            logger.warning("Overwrite download set but not overwrite processing.")

        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Processed layer exists: {input_path}")

        else:
            logger.info(f"Processing: {input_path}")

            tmp_input_path = self.process_dir / Path(input_path).name
            tmp_output_path = self.process_dir / Path(output_path).name

            logger.info(f"Copying input to tmp {input_path} {tmp_input_path}")
            shutil.copyfile(input_path, tmp_input_path)

            logger.info(f"Running raster calc {tmp_input_path} {tmp_output_path}")
            netcdf_path = f"netcdf:{tmp_input_path}:lccs_class"

            default_meta = {
                # 'count': 1,
                # 'crs': {'init': 'epsg:4326'},
                'driver': 'GTiff',
                'compress': 'LZW',
                # 'nodata': -9999,
            }

            with rasterio.open(netcdf_path) as src:
                assert len(set(src.block_shapes)) == 1
                meta = src.meta.copy()
                meta.update(**default_meta)
                with rasterio.open(tmp_output_path, "w", **meta) as dst:
                    for ji, window in src.block_windows(1):
                        in_data = src.read(window=window)
                        out_data = self.map_func(in_data)
                        out_data = out_data.astype(meta["dtype"])
                        dst.write(out_data, window=window)

            logger.info(f"Copying output tmp to final {tmp_output_path} {output_path}")
            shutil.copyfile(tmp_output_path, output_path)

        return


    def main(self):
        logger = self.get_logger()

        os.makedirs(self.raw_dir / "compressed", exist_ok=True)
        os.makedirs(self.raw_dir / "uncompressed", exist_ok=True)

        # Download data
        logger.info("Running data download")
        download = self.run_tasks(self.download, [[y] for y in self.years])
        self.log_run(download)

        os.makedirs(self.output_dir, exist_ok=True)

        # Process data
        logger.info("Running processing")
        process_inputs = zip(download.results(), [self.output_dir / f"esa_lc_{year}.tif" for year in self.years])
        process = self.run_tasks(self.process, process_inputs)
        self.log_run(process)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "process_dir": Path(config["main"]["process_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs"
    }


if __name__ == "__main__":

    config_dict = get_config_dict()

    log_dir = config_dict["log_dir"]
    timestamp = datetime.today()
    time_format_str: str="%Y_%m_%d_%H_%M"
    time_str = timestamp.strftime(time_format_str)
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)


    class_instance = ESALandcover(config_dict["raw_dir"], config_dict["process_dir"], config_dict["output_dir"], config_dict["years"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)