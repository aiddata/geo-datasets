"""

1. Set the raw and output data directories in the config
2. Go to https://landscan.ornl.gov and manually download all desired years of the "LandScan Global" population dataset
3. Make sure all files are downloaded to a folder named "compressed" within the raw_data directory path specified in your config
4. Edit the years in the config if needed (if you only downloaded a subset of years, or only want to extract/process a subset of years)


"""

import os
import sys
import zipfile
import glob
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser

import rasterio


sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class LandScanPop(Dataset):
    name = "LandScan Population"

    def __init__(self, raw_dir, output_dir, years, run_extract=True, run_conversion=True, overwrite_extract=False, overwrite_conversion=False):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)

        self.years = years

        self.run_extract = run_extract
        self.run_conversion = run_conversion

        self.overwrite_extract = overwrite_extract
        self.overwrite_conversion = overwrite_conversion

        self.download_dir = self.raw_dir / "compressed"
        self.extract_dir = self.raw_dir / "uncompressed"

        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)



    def unzip_file(self, zip_file, out_dir):
        """Extract a zipfile"""
        logger = self.get_logger()
        if os.path.isdir(out_dir) and not self.overwrite_extract:
            logger.info(f"Extracted directory exists - skipping ({out_dir})")
        else:
            logger.info(f"Extracting {zip_file} to {out_dir}")
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(out_dir)


    def convert_to_cog(self, src, dst):
        """Convert a raster from ESRI grid format to COG format"""
        logger = self.get_logger()

        if os.path.isfile(dst) and not self.overwrite_conversion:
            logger.info(f"COG exists - skipping ({dst})")
        else:
            logger.info(f"Converting to COG ({dst})")
            with rasterio.open(src) as src:
                assert len(set(src.block_shapes)) == 1
                meta = src.meta.copy()
                meta.update({
                    'driver': 'COG',
                    'compress': 'LZW',
                })

                with rasterio.open(dst, "w", **meta) as dst:
                    for ji, window in src.block_windows(1):
                        in_data = src.read(window=window)
                        dst.write(in_data, window=window)


    def build_extract_list(self):
        """Build a list of files to extract"""
        flist = []
        for x in self.download_dir.iterdir():
            y = int(x.name.split("-")[2])
            if x.name.endswith(".zip") and y in self.years:
                flist.append(( self.download_dir / x, self.extract_dir / x.name[:-4] ))

        return flist


    def build_conversion_list(self):
        """Build a list of files to convert"""
        flist = []
        for x in self.extract_dir.iterdir():
            y = int(x.name.split("-")[2])
            if os.path.isdir(x) and y in self.years:
                flist.append(( x / f"{x.name}.tif" , self.data_dir / f"{x.name}.tif" ))

        return flist

    def main(self):
        logger = self.get_logger()

        logger.info('Starting pipeline...')

        # unzip
        if self.run_extract:
            logger.info('Running extract tasks...')
            ex_list = self.build_extract_list()
            extract = self.run_tasks(self.unzip_file, ex_list)
            self.log_run(extract)


        # convert from esri grid format to COG
        if self.run_conversion:
            logger.info('Running conversion tasks...')
            conv_list = self.build_conversion_list()
            conv = self.run_tasks(self.convert_to_cog, conv_list)
            self.log_run(conv)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "run_extract": config["main"].getboolean("run_extract"),
        "run_conversion": config["main"].getboolean("run_conversion"),
        "overwrite_extract": config["main"].getboolean("overwrite_extract"),
        "overwrite_conversion": config["main"].getboolean("overwrite_conversion"),
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


    class_instance = LandScanPop(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["run_extract"], config_dict["run_conversion"], config_dict["overwrite_extract"], config_dict["overwrite_conversion"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
