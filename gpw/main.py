import os
import sys
import zipfile
import requests
from copy import copy
from pathlib import Path
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser


sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset



class GPWv4(Dataset):
    name = "GPWv4"

    def __init__(self,
                 raw_dir: str,
                 output_dir: str,
                 years: list,
                 sedac_cookie: str,
                 overwrite_download: bool = False,
                 overwrite_extract: bool = False,
                 overwrite_processing: bool = False):
        """
        :param raw_dir: directory to download files to
        :param output_dir: directory to unzip files to
        :param years: list of years to download
        :param sedac_cookie: sedac cookie value acquired using steps documented in readme
        :param only_unzip: if you already downloaded files and just want to unzip them, set this to true
        :param overwrite_download: if you want to overwrite files that have already been downloaded, set this to true
        :param overwrite_extract: if you want to overwrite files that have already been extracted, set this to true
        :param overwrite_processing: if you want to overwrite files that have already been processed, set this to true
        """

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)

        self.years = [int(year) for year in years]

        self.sedac_cookie = sedac_cookie

        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract
        self.overwrite_processing = overwrite_processing



    def download_docs(self):
        documentation_url = "https://sedac.ciesin.columbia.edu/downloads/docs/gpw-v4/gpw-v4-documentation-rev11.zip"

        # download documentation
        response = requests.get(documentation_url, headers={'Cookie': f'sedac={self.sedac_cookie}'}, allow_redirects=True)
        open(f"{self.raw_dir}/gpw-v4-documentation-rev11.zip", 'wb').write(response.content)


    def build_download_list(self):

        task_list = []

        for var in ["density", "count"]:

            # path to download/extract files to
            var_dl_dir = self.raw_dir / 'download' / var
            var_extract_dir = self.raw_dir / 'extract' / var

            var_dl_dir.mkdir(parents=True, exist_ok=True)
            var_extract_dir.mkdir(parents=True, exist_ok=True)

            var_base_url = f"https://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-{var}-adjusted-to-2015-unwpp-country-totals-rev11"

            for year in self.years:

                dl_src = f"{var_base_url}/gpw-v4-population-{var}-adjusted-to-2015-unwpp-country-totals-rev11_{year}_30_sec_tif.zip"
                dl_dst = var_dl_dir / Path(dl_src).name
                extract_dst = var_extract_dir

                task_list.append((dl_src, dl_dst, extract_dst))

        return task_list


    def download(self, src, dst, extract_dir):

        logger = self.get_logger()

        if not dst.exists() or self.overwrite_download:
            logger.info(f"Downloading {src}")
            response = requests.get(src, headers={'Cookie': f'sedac={self.sedac_cookie}'}, allow_redirects=True)
            with open(dst, 'wb') as dst_file:
                dst_file.write(response.content)
        else:
            logger.info(f"Download Exists {src}")


        with zipfile.ZipFile(dst, 'r') as zip_ref:
            zip_member = [member for member in zip_ref.namelist() if member.endswith('.tif')][0]
            if not (extract_dir / zip_member).exists() or self.overwrite_extract:
                logger.info(f"Extracting {dst}")
                zip_ref.extract(zip_member, path=extract_dir)
            else:
                logger.info(f"Extract Exists {dst}")


    def create_process_list(self):
        logger = self.get_logger()

        flist = []

        for var in ["density", "count"]:

            var_final_dir = self.output_dir / var
            var_final_dir.mkdir(parents=True, exist_ok=True)

            var_extract_dir = self.raw_dir / 'extract' / var

            extracted_files = [i for i in var_extract_dir.iterdir() if str(i).endswith('.tif')]
            for i in extracted_files:
                year = int(i.name.split('_')[-3])
                if year in self.years:
                    flist.append((i, var_final_dir / i.name))

        # logger.info(f"COG conversion list: {flist}")

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

        # logger.info("Download documentation")
        # self.download_docs()

        # logger.info("Building download list")
        # dl_list = self.build_download_list()

        # logger.info("Running download and extract")
        # dl = self.run_tasks(self.download, dl_list)
        # self.log_run(dl)

        logger.info("Converting to COG")
        cog_list = self.create_process_list()
        cog = self.run_tasks(self.convert_to_cog, cog_list)
        self.log_run(cog)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "sedac_cookie": config["main"]["sedac_cookie"],
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_extract": config["main"].getboolean("overwrite_extract"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs",
    }


if __name__ == "__main__":

    config_dict = get_config_dict()

    log_dir = config_dict["log_dir"]
    timestamp = datetime.today()
    time_format_str: str="%Y_%m_%d_%H_%M"
    time_str = timestamp.strftime(time_format_str)
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)


    class_instance = GPWv4(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["sedac_cookie"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
