import os
import zipfile
from copy import copy
from pathlib import Path
from typing import List, Union

import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class GPWConfiguration(BaseDatasetConfiguration):
    token: str
    raw_dir: str
    output_dir: str
    # Comma-separated years (e.g. "2000,2001"). String, not list, so the
    # Prefect run form renders a text input rather than the array widget,
    # whose "add item" button submits the form.
    years: str
    overwrite_download: bool
    overwrite_extract: bool
    overwrite_processing: bool


class GPWv4(Dataset):
    name = "GPWv4"

    def __init__(self, config: GPWConfiguration):
        """
        :param raw_dir: directory to download files to
        :param output_dir: directory to unzip files to
        :param years: list of years to download
        :param token: Earthdata token for authentication
        :param only_unzip: if you already downloaded files and just want to unzip them, set this to true
        :param overwrite_download: if you want to overwrite files that have already been downloaded, set this to true
        :param overwrite_extract: if you want to overwrite files that have already been extracted, set this to true
        :param overwrite_processing: if you want to overwrite files that have already been processed, set this to true
        """

        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)

        self.years = [int(v.strip()) for v in config.years.split(",") if v.strip()]

        self.auth_headers = {"Authorization": f"Bearer {config.token}"}

        self.overwrite_download = config.overwrite_download
        self.overwrite_extract = config.overwrite_extract
        self.overwrite_processing = config.overwrite_processing


    def build_download_list(self):

        task_list = []

        for var in ["density", "count"]:

            # path to download/extract files to
            var_dl_dir = self.raw_dir / "download" / var
            var_extract_dir = self.raw_dir / "extract" / var

            var_dl_dir.mkdir(parents=True, exist_ok=True)
            var_extract_dir.mkdir(parents=True, exist_ok=True)

            # var_base_url = f"https://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-{var}-adjusted-to-2015-unwpp-country-totals-rev11"
            var_base_url = f"https://data.earthdata.nasa.gov/nasa-earth/human-dimensions/sedac-root/downloads/data/gpw-v4/gpw-v4-population-{var}-adjusted-to-2015-unwpp-country-totals-rev11"

            for year in self.years:

                dl_src = f"{var_base_url}/gpw-v4-population-{var}-adjusted-to-2015-unwpp-country-totals-rev11_{year}_30_sec_tif.zip"
                dl_dst = var_dl_dir / Path(dl_src).name
                extract_dst = var_extract_dir

                task_list.append((dl_src, dl_dst, extract_dst))

        return task_list

    def download_and_extract(self, src_url: str, dst_path: Union[str, os.PathLike], extract_dir: Union[str, os.PathLike]) -> None:
        self.download(src_url, dst_path)
        self.extract(dst_path, extract_dir)

    def download(self, src_url: str, dst_path: Union[str, os.PathLike]) -> None:
        logger = self.get_logger()
        logger.info(f"Downloading {str(dst_path)}...")
        with requests.get(src_url, headers=self.auth_headers, stream=True) as src:
            src.raise_for_status()
            with self.tmp_to_dst_file(dst_path, make_dst_dir=True) as dst_path:
                with open(dst_path, "wb") as dst:
                    for chunk in src.iter_content(chunk_size=8192):
                        dst.write(chunk)

    def extract(self, zip_path: Union[str, os.PathLike], extract_dir: Union[str, os.PathLike]) -> None:
        logger = self.get_logger()
        logger.info(f"Extracting {str(zip_path)} to {str(extract_dir)}...")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_member = [
                member for member in zip_ref.namelist() if member.endswith(".tif")
            ][0]
            if not (extract_dir / zip_member).exists() or self.overwrite_extract:
                logger.info(f"Extracting {zip_member}")
                zip_ref.extract(zip_member, path=extract_dir)
            else:
                logger.info(f"Extract exists for {zip_member}")


    def create_process_list(self):
        logger = self.get_logger()

        flist = []

        for var in ["density", "count"]:

            var_final_dir = self.output_dir / var
            var_final_dir.mkdir(parents=True, exist_ok=True)

            var_extract_dir = self.raw_dir / "extract" / var

            extracted_files = [
                i for i in var_extract_dir.iterdir() if str(i).endswith(".tif")
            ]
            for i in extracted_files:
                year = int(i.name.split("_")[-3])
                if year in self.years:
                    flist.append((i, var_final_dir / i.name))

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

                # print(profile)
                # logger.info(profile)

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

        logger.info("Building download list...")
        dl_list = self.build_download_list()

        logger.info("Running download and extract...")
        dl = self.run_tasks(self.download_and_extract, dl_list)
        self.log_run(dl)

        logger.info("Building COG conversion list...")
        cog_list = self.create_process_list()
        logger.info("Converting to COG...")
        cog = self.run_tasks(self.convert_to_cog, cog_list)
        self.log_run(cog)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def gpwv4(config: GPWConfiguration):
        GPWv4(config).run(config.run)


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()
    config = get_config(GPWConfiguration)
    # secrets come from the gitignored .env for local runs
    config.token = os.environ.get("EARTHDATA_TOKEN")
    GPWv4(config).run(config.run)
