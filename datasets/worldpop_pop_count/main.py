"""

worldpop: https://www.worldpop.org/geodata/listing?id=64

"""

import os
from copy import copy
from pathlib import Path
from typing import List

import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class WorldPopCountConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    years: List[int]
    overwrite_download: bool
    overwrite_processing: bool


class WorldPopCount(Dataset):
    name = "WorldPop Count"

    def __init__(
        self,
        config: WorldPopCountConfiguration,
    ):

        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = config.years
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

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

    def download_file(self, url, local_filename):
        """Download a file from url to local_filename
        Downloads in chunks
        """
        with requests.get(url, stream=True, verify=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

    def create_process_list(self):
        logger = self.get_logger()

        flist = []
        downloaded_files = [
            i for i in self.raw_dir.iterdir() if str(i).endswith(".tif")
        ]
        for i in downloaded_files:
            year = int(i.name.split("_")[1])
            if year in self.years:
                flist.append((i, self.output_dir / i.name))

        logger.info(f"COG conversion list: {flist}")

        return flist

    def convert_to_cog(self, src_path, dst_path):
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
        """

        import rasterio
        from rasterio import windows

        logger = self.get_logger()

        if not self.overwrite_processing and dst_path.exists():
            logger.info(f"COG Exists: {dst_path}")

        else:

            logger.info(f"Generating COG: {dst_path}")

            with rasterio.open(src_path, "r") as src:

                profile = copy(src.profile)

                profile.update(
                    {
                        "driver": "COG",
                        "compress": "LZW",
                    }
                )

                # These creation options are not supported by the COG driver
                for k in ["BLOCKXSIZE", "BLOCKYSIZE", "TILED", "INTERLEAVE"]:
                    if k in profile:
                        del profile[k]

                print(profile)
                logger.info(profile)

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


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def worldpop_pop_count(config: WorldPopCountConfiguration):
        WorldPopCount(config).run(config.run)


if __name__ == "__main__":
    config = get_config(WorldPopCountConfiguration)
    WorldPopCount(config).run(config.run)
