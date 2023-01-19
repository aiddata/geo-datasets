"""

worldpop: https://www.worldpop.org/geodata/listing?id=65

"""

import os
import sys
import requests
import shutil
from copy import copy
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class WorldPopAgeSex(Dataset):
    name = "WorldPop Age Sex"

    def __init__(self, process_dir, raw_dir, output_dir, years, overwrite_download=False, overwrite_processing=False):

        self.process_dir = Path(process_dir)
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing

        self.template_url = "https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020/{YEAR}/0_Mosaicked/global_mosaic_1km/global_{SEX}_{AGE}_{YEAR}_1km.tif"

        self.template_download_dir_basename = "{SEX}_{AGE}"


        self.age_list = [0, 1]
        # for k in range(5, 85, 5):
        #     self.age_list.append(k)

        self.sex_list =  ["m", "f"]


    def test_connection(self):
        # test connection
        test_request = requests.get("https://data.worldpop.org/GIS/", verify=True)
        test_request.raise_for_status()


    def create_download_list(self):

        flist = []

        for sex in self.sex_list:
            for age in self.age_list:
                download_dir = self.template_download_dir_basename.format(SEX = sex, AGE = age)
                for year in self.years:
                    src_url = self.template_url.format(SEX = sex, AGE = age, YEAR = year)
                    tmp_path = self.process_dir / 'download' / download_dir / os.path.basename(src_url)
                    dst_path = self.raw_dir / download_dir / os.path.basename(src_url)
                    flist.append((src_url, tmp_path, dst_path))

        return flist


    def manage_download(self, url, tmp_path, dst_path):
        """download individual file using session created
        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """
        logger = self.get_logger()

        max_attempts = 5
        if os.path.isfile(dst_path) and not self.overwrite_download:
            logger.info(f"Download Exists: {url}")

        else:
            Path(tmp_path).parent.mkdir(parents=True, exist_ok=True)

            attempts = 1
            while attempts <= max_attempts:
                try:
                    self.download_file(url, tmp_path)
                except Exception as e:
                    attempts += 1
                    if attempts > max_attempts:
                        raise e
                else:
                    logger.info(f"Downloaded to tmp: {url}")
                    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
                    self.move_file(tmp_path, dst_path)
                    logger.info(f"Copied to dst: {url}")


    def download_file(self, url, tmp_path):
        """Download a file from url to tmp_path
        Downloads in chunks
        """
        with requests.get(url, stream=True, verify=True) as r:
            r.raise_for_status()
            with open(tmp_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)


    def move_file(self, src, dst):
        shutil.copyfile(src, dst)


    def create_process_list(self):
        logger = self.get_logger()

        (self.process_dir / 'cog_tmp').mkdir(parents=True, exist_ok=True)
        (self.output_dir).mkdir(parents=True, exist_ok=True)

        flist = []
        downloaded_files = [i for i in self.raw_dir.iterdir() if str(i).endswith('.tif')]
        for i in downloaded_files:
            year = int(i.name.split('_')[1])
            if year in self.years:
                flist.append((i, self.process_dir / 'cog_tmp' / i.name, self.output_dir / i.name))

        logger.info(f"COG conversion list: {flist}")

        return flist


    def convert_to_cog(self, src_path, tmp_path, dst_path):
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
        """

        import rasterio
        from rasterio import windows

        logger = self.get_logger()

        if not self.overwrite_processing and dst_path.exists():
            logger.info(f"COG Exists: {dst_path}")

        else:

            logger.info(f"Generating COG: {tmp_path} / {dst_path}")

            with rasterio.open(src_path, 'r') as src:

                profile = copy(src.profile)

                profile.update({
                    'driver': 'COG',
                    'compress': 'LZW',
                })

                # These creation options are not supported by the COG driver
                for k in ["BLOCKXSIZE", "BLOCKYSIZE", "TILED", "INTERLEAVE"]:
                    if k in profile:
                        del profile[k]

                print(profile)
                logger.info(profile)

                with rasterio.open(tmp_path, 'w+', **profile) as dst:

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


            logger.info(f"Copying COG to final dst: {dst_path}")
            self.move_file(tmp_path, dst_path)


    def main(self):

        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        logger.info("Preparing for data download")
        download_flist = self.create_download_list()
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Running data download")
        downloads = self.run_tasks(self.manage_download, download_flist)
        self.log_run(downloads, expand_args=["url", "download_path"])

        logger.info("Preparing for processing")
        process_flist = self.create_process_list()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Converting raw tifs to COGs")
        conversions = self.run_tasks(self.convert_to_cog, process_flist)
        self.log_run(conversions, expand_args=["src_path", "dst_path"])


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "process_dir": Path(config["main"]["process_dir"]),
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

    log_dir = config_dict["log_dir"]
    timestamp = datetime.today()
    time_format_str: str="%Y_%m_%d_%H_%M"
    time_str = timestamp.strftime(time_format_str)
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)


    class_instance = WorldPopAgeSex(config_dict["process_dir"], config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
