# data download script for DVNL ntl data 
# info link: https://eogdata.mines.edu/products/dmsp/#dvnl 

import os
import sys
import requests
import rasterio
from rasterio import windows
from copy import copy
from pathlib import Path
from configparser import ConfigParser

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class DVNL(Dataset):

    name = "DVNL"

    def __init__(self, raw_dir, output_dir, years, overwrite_download=False, overwrite_processing=False):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing
        self.download_url = "https://eogdata.mines.edu/wwwdata/viirs_products/dvnl/DVNL_{YEAR}.tif"
    
    def test_connection(self):
        # test connection
        test_request = requests.get("https://eogdata.mines.edu/wwwdata/viirs_products/dvnl/", verify=True)
        test_request.raise_for_status()


    def manage_download(self, year):
        """
        Download individual file
        """

        logger = self.get_logger()

        download_dest = self.download_url.format(YEAR = year)
        local_filename = self.raw_dir / f"raw_dvnl_{year}.tif"

        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {download_dest}")
        else:
            with requests.get(download_dest, stream=True, verify=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)
            logger.info(f"Downloaded: {download_dest}")

        return (download_dest, local_filename)


    def convert_to_cog(self, year):
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
        """
        logger = self.get_logger()

        src_path = self.raw_dir / f"raw_dvnl_{year}.tif"
        dst_path = self.output_dir / f"dvnl_{year}.tif"

        if os.path.isfile(dst_path) and not self.overwrite_processing:
            logger.info(f"Converted File Exists: {dst_path}")
            return (src_path, dst_path)

        else:
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
            logger.info(f"File Converted: {dst_path}")
            return (src_path, dst_path)


    def main(self):

        logger = self.get_logger()

        os.makedirs(self.raw_dir, exist_ok=True)

        logger.info("Testing Connection...")
        self.test_connection()

        logger.info("Running data download")
        download = self.run_tasks(self.manage_download, [[y] for y in self.years])
        self.log_run(download)

        os.makedirs(self.output_dir, exist_ok=True)

        logger.info("Converting raw tifs to COGs")
        conversions = self.run_tasks(self.convert_to_cog, [[y] for y in self.years])
        self.log_run(conversions)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "years": [int(y) for y in config["main"]["years"].split(", ")],
            "log_dir": Path(config["main"]["output_dir"]) / "logs", 
            "backend": config["run"]["backend"],
            "task_runner": config["run"]["task_runner"],
            "run_parallel": config["run"].getboolean("run_parallel"),
            "max_workers": int(config["run"]["max_workers"]),
            "cores_per_process": int(config["run"]["cores_per_process"]),
            "overwrite_download": config["main"].getboolean("overwrite_download"),
            "overwrite_processing": config["main"].getboolean("overwrite_processing")
        }

if __name__ == "__main__":
    config_dict = get_config_dict()

    class_instance = DVNL(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])
