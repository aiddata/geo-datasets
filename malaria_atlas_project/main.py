"""
Download and prepare data from the Malaria Atlas Project

https://data.malariaatlas.org

Data is no longer directly available from a file server but is instead provided via a GeoServer instance.
This can still be retrieved using a standard request call but to determine the proper URL requires initiating the download via their website mapping platform and tracing the corresponding network call. This process only needs to be done once for new datasets; URLs for the datasets listed below have already been identifed.


Current download options:
- pf_incidence_rate: incidence data for Plasmodium falciparum species of malaria (rate per 1000 people)


Unless otherwise specified, all datasets are:
- global
- from 2000-2020
- downloaded as a single zip file containing each datasets (all data in zip root directory)
- single band LZW-compressed GeoTIFF files at 2.5 arcminute resolution
"""

import os
import shutil
import requests
from copy import copy
from pathlib import Path
from zipfile import ZipFile
from configparser import ConfigParser

import rasterio
from rasterio import windows

from dataset import Dataset


class MalariaAtlasProject(Dataset):
    name = "Malaria Atlas Project"

    def __init__(self, raw_dir, output_dir, years, dataset="pf_incidence_rate", overwrite=False):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.dataset = dataset
        self.overwrite = overwrite

        dataset_lookup = {
            "pf_incidence_rate": {
                "data_zipFile_url": 'https://data.malariaatlas.org/geoserver/Malaria/ows?service=CSW&version=2.0.1&request=DirectDownload&ResourceId=Malaria:202206_Global_Pf_Incidence_Rate',
                "data_name": "202206_Global_Pf_Incidence_Rate"
            },
        }

        self.data_info = dataset_lookup[self.dataset]



    def test_connection(self):
        # test connection
        test_request = requests.get("https://data.malariaatlas.org", verify=True)
        test_request.raise_for_status()


    def copy_files(self, zip_path, zip_file, dst_path, cog_path, overwrite=False):
        if not os.path.isfile(dst_path) or self.overwrite:
            with ZipFile(zip_path) as myzip:
                with myzip.open(zip_file) as src:
                    with open(dst_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)

            if not os.path.isfile(dst_path):
                raise Exception("File extracted but not found at destination")

        return (dst_path, cog_path)


    def convert_to_cog(self, src_path, dst_path):
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
        """
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

    def copy_data_files(self, zip_file_local_name):

        logger = self.get_logger()

        # create zipFile to check if data was properly downloaded
        try:
            dataZip = ZipFile(zip_file_local_name)
        except:
            print("Could not read downloaded zipfile")
            raise

        raw_geotiff_dir = self.raw_dir / "geotiff" / self.dataset
        raw_geotiff_dir.mkdir(parents=True, exist_ok=True)

        # validate years for processing
        zip_years = sorted([int(i[-8:-4]) for i in dataZip.namelist() if i.endswith('.tif')])
        year_list = self.years
        years = [i for i in year_list if i in zip_years]
        if len(year_list) != len(years):
            missing_years = set(year_list).symmetric_difference(set(years))
            logger.warning(f"Years not found in downloaded data {missing_years}")

        flist = []
        for year in years:
            year_file_name = self.data_info["data_name"] + f"_{year}.tif"

            tif_path = raw_geotiff_dir / year_file_name
            cog_path = self.output_dir / year_file_name

            flist.append((zip_file_local_name, year_file_name, tif_path, cog_path))

        return flist

    def download_file(self, url, local_filename):
        """Download a file from url to local_filename
        Downloads in chunks
        """
        with requests.get(url, stream=True, verify=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)

    def manage_download(self, url, local_filename, overwrite=False):
        """download individual file using session created
        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """
        logger = self.get_logger()

        max_attempts = 5
        if os.path.isfile(local_filename) and not overwrite:
            logger.info(f"Download Exists: {url}")
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
                    return

    def main(self):

        logger = self.get_logger()

        raw_zip_dir = self.raw_dir / "zip" / self.dataset
        raw_zip_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Testing Connection123...")
        self.test_connection()

        logger.info("Running data download")
        zip_file_local_name = raw_zip_dir / (self.data_info["data_name"] + ".zip")
        # download data zipFile from url to the local output directory
        downloads = self.run_tasks(self.manage_download, [(self.data_info["data_zipFile_url"], zip_file_local_name)])
        self.log_run(downloads)

        logger.info("Copying data files")
        file_copy_list = self.copy_data_files(zip_file_local_name)
        copy_futures = self.run_tasks(self.copy_files, file_copy_list)
        self.log_run(copy_futures)

        conversions = self.run_tasks(self.convert_to_cog, copy_futures)
        self.log_run(conversions)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "dataset": config["Config"]["dataset"],
        "years": [int(y) for y in config["Config"]["years"].split(", ")],
        "raw_dir": Path(config["Config"]["raw_dir"]),
        "output_dir": Path(config["Config"]["output_dir"]),
        "overwrite": config["Config"].getboolean("overwrite"),
        "backend": config["Config"]["backend"],
        "task_runner": config["Config"]["task_runner"],
        "run_parallel": config["Config"].getboolean("run_parallel"),
        "max_workers": int(config["Config"]["max_workers"]),
        "log_dir": Path(config["Config"]["raw_dir"]) / "logs"
    }

if __name__ == "__main__":
    MalariaAtlasProject(**get_config_dict()).run()
