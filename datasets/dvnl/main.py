# data download script for DVNL ntl data
# info link: https://eogdata.mines.edu/products/dmsp/#dvnl
import os
from copy import copy
from pathlib import Path
from typing import List

import rasterio
import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from rasterio import windows


class DVNLConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    years: List[int]
    overwrite_download: bool
    overwrite_processing: bool


class DVNL(Dataset):

    name = "DVNL"

    def __init__(self, config: DVNLConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = config.years
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing
        self.download_url = (
            "https://eogdata.mines.edu/wwwdata/viirs_products/dvnl/DVNL_{YEAR}.tif"
        )

    def test_connection(self):
        # test connection
        test_request = requests.get(
            "https://eogdata.mines.edu/wwwdata/viirs_products/dvnl/", verify=True
        )
        test_request.raise_for_status()

    def manage_download(self, year):
        """
        Download individual file
        """

        logger = self.get_logger()

        download_dest = self.download_url.format(YEAR=year)
        local_filename = self.raw_dir / f"raw_dvnl_{year}.tif"

        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {download_dest}")
        else:
            with requests.get(download_dest, stream=True, verify=True) as r:
                r.raise_for_status()
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
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
            with rasterio.open(src_path, "r") as src:

                profile = copy(src.profile)

                profile.update(
                    {
                        "driver": "COG",
                        "compress": "LZW",
                    }
                )

                with rasterio.open(dst_path, "w+", **profile) as dst:

                    for ji, src_window in src.block_windows(1):
                        # convert relative input window location to relative output window location
                        # using real world coordinates (bounds)
                        src_bounds = windows.bounds(
                            src_window, transform=src.profile["transform"]
                        )
                        dst_window = windows.from_bounds(
                            *src_bounds, transform=dst.profile["transform"]
                        )
                        # round the values of dest_window as they can be float
                        dst_window = windows.Window(
                            round(dst_window.col_off),
                            round(dst_window.row_off),
                            round(dst_window.width),
                            round(dst_window.height),
                        )
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


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def dvnl(config: DVNLConfiguration):
        DVNL(config).run(config.run)


if __name__ == "__main__":
    config = get_config(DVNLConfiguration)
    DVNL(config).run(config.run)
