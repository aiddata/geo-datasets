"""

worldpop: https://www.worldpop.org/geodata/listing?id=65

"""

import os
import shutil
from copy import copy
from pathlib import Path
from typing import List

import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class WorldPopAgeSexConfiguration(BaseDatasetConfiguration):
    process_dir: str
    raw_dir: str
    output_dir: str
    years: List[int]
    overwrite_download: bool
    overwrite_processing: bool


class WorldPopAgeSex(Dataset):
    name = "WorldPop Age Sex"

    def __init__(
        self,
        config: WorldPopAgeSexConfiguration,
    ):
        self.process_dir = Path(config.process_dir)
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = config.years
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

        self.template_url = "https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020/{YEAR}/0_Mosaicked/global_mosaic_1km/global_{SEX}_{AGE}_{YEAR}_1km.tif"

        self.template_download_dir_basename = "{SEX}_{AGE}"

        self.age_list = [0, 1]
        for k in range(5, 85, 5):
            self.age_list.append(k)

        self.sex_list = ["m", "f"]

    def test_connection(self):
        # test connection
        test_request = requests.get("https://data.worldpop.org/GIS/", verify=True)
        test_request.raise_for_status()

    def create_download_list(self):
        flist = []

        for sex in self.sex_list:
            for age in self.age_list:
                download_dir = self.template_download_dir_basename.format(
                    SEX=sex, AGE=age
                )
                for year in self.years:
                    src_url = self.template_url.format(SEX=sex, AGE=age, YEAR=year)
                    tmp_path = (
                        self.process_dir
                        / "download"
                        / download_dir
                        / os.path.basename(src_url)
                    )
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
                    return

    def download_file(self, url, tmp_path):
        """Download a file from url to tmp_path
        Downloads in chunks
        """
        user_agent_str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        with requests.get(
            url, stream=True, verify=True, headers={"User-Agent": user_agent_str}
        ) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

    def move_file(self, src, dst):
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)

    def create_process_list(self):
        logger = self.get_logger()

        flist = []
        # downloaded_files = [i for i in self.raw_dir.iterdir() if str(i).endswith('.tif')]
        downloaded_files = list(self.raw_dir.glob("**/*.tif"))

        for i in downloaded_files:
            sex = i.name.split("_")[1]
            age = int(i.name.split("_")[2])
            year = int(i.name.split("_")[3])
            if sex in self.sex_list and age in self.age_list and year in self.years:
                flist.append(
                    (
                        i,
                        self.process_dir / "cog_tmp" / i.name,
                        self.output_dir / f"{sex}_{age}" / i.name,
                    )
                )

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
            (self.process_dir / "cog_tmp").mkdir(parents=True, exist_ok=True)
            (self.output_dir).mkdir(parents=True, exist_ok=True)

            logger.info(f"Generating COG: {tmp_path} / {dst_path}")

            with rasterio.open(src_path, "r") as src:
                profile = copy(src.profile)

                profile.update(
                    {
                        "driver": "COG",
                        "compress": "LZW",
                        # 'BLOCKXSIZE': 512,
                        # 'BLOCKYSIZE': 512,
                        # 'TILED': True,
                        # 'INTERLEAVE': 'BAND',
                    }
                )

                # These creation options are not supported by the COG driver
                # for k in ["BLOCKXSIZE", "BLOCKYSIZE", "TILED", "INTERLEAVE"]:
                #     if k in profile:
                #         del profile[k]

                print(profile)
                logger.info(profile)

                with rasterio.open(tmp_path, "w+", **profile) as dst:
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
        downloads = self.run_tasks(self.manage_download, download_flist, prefect_concurrency_tag="worldpop_download", prefect_concurrency_task_value=1)
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
    def worldpop_age_sex(config: WorldPopAgeSexConfiguration):
        WorldPopAgeSex(config).run(config.run)


if __name__ == "__main__":
    config = get_config(WorldPopAgeSexConfiguration)
    WorldPopAgeSex(config).run(config.run)
