"""

WorldPop population count, global 1km mosaics 2015-2030 (R2025A v1)

worldpop: https://hub.worldpop.org/geodata/listing?id=137

Constrained population counts mosaiced from the 100m country datasets. Years
past the release date are projections. An optional UN-adjusted variant is
available via the `un_adjusted` config flag.

"""

import os
from copy import copy
from pathlib import Path

import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class WorldPopCountConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    # Comma-separated years (e.g. "2015,2016"). Kept as a string rather than a
    # list so the Prefect run form renders a text input instead of the array
    # widget, whose "add item" button submits the form.
    years: str
    # If true, download the UN-adjusted (UA) variant of the mosaics.
    un_adjusted: bool = False
    overwrite_download: bool
    overwrite_processing: bool


class WorldPopCount(Dataset):
    name = "WorldPop Count R2025A"

    def __init__(
        self,
        config: WorldPopCountConfiguration,
    ):

        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = [int(y.strip()) for y in config.years.split(",") if y.strip()]
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

        if config.un_adjusted:
            self.template_url = "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{YEAR}/0_Mosaicked/v1/1km_ua/constrained/global_pop_{YEAR}_CN_1km_R2025A_UA_v1.tif"
        else:
            self.template_url = "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{YEAR}/0_Mosaicked/v1/1km/constrained/global_pop_{YEAR}_CN_1km_R2025A_v1.tif"

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
        Downloads in chunks. The file is written to a temporary path and only
        moved to local_filename once complete, so an interrupted download
        cannot leave a partial file that a later run would mistake for a
        complete one. tmp_dir is on the same filesystem as the destination:
        the final move stays a cheap rename, and large files don't accumulate
        in the pod's local /tmp.
        """
        with self.tmp_to_dst_file(local_filename, tmp_dir=self.raw_dir) as tmp_path:
            with requests.get(url, stream=True, verify=True) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)

    def create_process_list(self):
        logger = self.get_logger()

        flist = []
        downloaded_files = [
            i for i in self.raw_dir.iterdir() if str(i).endswith(".tif")
        ]
        for i in downloaded_files:
            # e.g. global_pop_2020_CN_1km_R2025A_v1.tif -> 2020
            year = int(i.name.split("_")[2])
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

                # These creation options are not supported by the COG driver.
                # rasterio profile keys are lowercase; with the uppercase names
                # this loop never matched, and GDAL warned about the unsupported
                # options on every conversion (it ignored them, so outputs were
                # unaffected).
                for k in ["blockxsize", "blockysize", "tiled", "interleave"]:
                    if k in profile:
                        del profile[k]

                logger.info(profile)

                with self.tmp_to_dst_file(
                    dst_path, tmp_dir=self.output_dir, validate_cog=True
                ) as tmp_dst_path, rasterio.open(tmp_dst_path, "w+", **profile) as dst:

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
    def worldpop_pop_count_r2025a(config: WorldPopCountConfiguration):
        WorldPopCount(config).run(config.run)


if __name__ == "__main__":
    config = get_config(WorldPopCountConfiguration)
    WorldPopCount(config).run(config.run)
