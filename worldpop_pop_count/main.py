"""

worldpop: https://www.worldpop.org/geodata/listing?id=64

"""

import os
import sys
import requests
from copy import copy
from pathlib import Path
from configparser import ConfigParser

import rasterio
from rasterio import windows


sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class WorldPopCount(Dataset):
    name = "WorldPop Count"

    def __init__(self, raw_dir, output_dir, years, overwrite_download=False, overwrite_processing=False):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing

        self.template_url = "https://data.worldpop.org/GIS/Population/Global_2000_2020/{YEAR}/0_Mosaicked/ppp_{YEAR}_1km_Aggregated.tif"


    def test_connection(self):
        # test connection
        test_request = requests.get("https://data.worldpop.org/GIS/", verify=True)
        test_request.raise_for_status()


    def create_download_list(self):

        flist = []
        for year in self.years:
            src_url = self.template_url.replace("{YEAR}", str(year))
            dst_path = os.path.join(self.raw_dir, os.path.basename(src_url))
            flist.append((src_url, dst_path))

        return flist


    def manage_download(self, url, local_filename):
        """download individual file using session created
        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """
        logger = self.get_logger()

        max_attempts = 5
        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {url}")
            raise
            return (url, local_filename)

        else:
            attempts = 1
            while attempts <= max_attempts:
                try:
                    self.download_file(url, local_filename)
                except Exception as e:
                    attempts += 1
                    if attempts > max_attempts:
                        raise e
                else:
                    logger.info(f"Downloaded: {url}")
                    return (url, local_filename)


    def download_file(self, url, local_filename):
        """Download a file from url to local_filename
        Downloads in chunks
        """
        with requests.get(url, stream=True, verify=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)


    def create_process_list(self):
        logger = self.get_logger()

        flist = []
        downloaded_files = [i for i in self.raw_dir.iterdir() if str(i).endswith('.tif')]
        for i in downloaded_files:
            year = int(i.name.split('_')[1])
            if year in self.years:
                flist.append((i, self.output_dir / i.name))

        logger.info(f"COG conversion list: {flist}")

        return flist


    def convert_to_cog(self, src_path, dst_path):
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
        """
        logger = self.get_logger()

        if not self.overwrite_processing and dst_path.exists():
            logger.info(f"COG Exists: {dst_path}")
            return

        logger.info(f"Generating COG: {dst_path}")

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


    def main(self):

        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        logger.info("Running data download")
        download_flist = self.create_download_list()
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        downloads = self.run_tasks(self.manage_download, download_flist)
        self.log_run(downloads, expand_args=["url", "download_path"])

        logger.info("Converting raw tifs to COGs")
        process_flist = self.create_process_list()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        conversions = self.run_tasks(self.convert_to_cog, process_flist)
        self.log_run(conversions)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
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

    class_instance = WorldPopCount(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=config_dict["log_dir"])
