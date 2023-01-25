import os
import sys
import zipfile
import requests
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
                 only_unzip: bool = False,
                 overwrite_download: bool = False,
                 overwrite_extract: bool = False):
        """
        :param raw_dir: directory to download files to
        :param output_dir: directory to unzip files to
        :param years: list of years to download
        :param sedac_cookie: sedac cookie value acquired using steps documented in readme
        :param only_unzip: if you already downloaded files and just want to unzip them, set this to true
        :param overwrite_download: if you want to overwrite files that have already been downloaded, set this to true
        :param overwrite_extract: if you want to overwrite files that have already been extracted, set this to true

        """

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)

        self.years = [int(year) for year in years]

        self.sedac_cookie = sedac_cookie
        self.only_unzip = only_unzip
        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract



    def download_docs(self):
        documentation_url = "https://sedac.ciesin.columbia.edu/downloads/docs/gpw-v4/gpw-v4-documentation-rev11.zip"

        # download documentation
        response = requests.get(documentation_url, headers={'Cookie': f'sedac={self.sedac_cookie}'}, allow_redirects=True)
        open(f"{self.raw_dir}/gpw-v4-documentation-rev11.zip", 'wb').write(response.content)


    def build_download_list(self):

        task_list = []

        for var in ["density", "count"]:

            # path to download/extract files to
            var_dl_dir = self.raw_dir / var
            var_final_dir = self.output_dir / var

            var_dl_dir.mkdir(parents=True, exist_ok=True)
            var_final_dir.mkdir(parents=True, exist_ok=True)

            var_base_url = f"https://sedac.ciesin.columbia.edu/downloads/data/gpw-v4/gpw-v4-population-{var}-adjusted-to-2015-unwpp-country-totals-rev11"

            for year in self.years:

                dl_src = f"{var_base_url}/gpw-v4-population-{var}-adjusted-to-2015-unwpp-country-totals-rev11_{year}_30_sec_tif.zip"
                dl_dst = var_dl_dir / Path(dl_src).name
                extract_dst = var_final_dir

                task_list.append((dl_src, dl_dst, extract_dst))

        return task_list


    def download(self, src, dst, extract_dir):

        if not self.only_unzip or not dst.exists():
            response = requests.get(src, headers={'Cookie': f'sedac={self.sedac_cookie}'}, allow_redirects=True)
            with open(dst, 'wb') as dst:
                dst.write(response.content)

        with zipfile.ZipFile(dst, 'r') as zip_ref:
            zip_ref.extractall(extract_dir, members=[member for member in zip_ref.namelist() if member.endswith('.tif')])


    def main(self):

        logger = self.get_logger()

        logger.info("Download documentation")
        self.download_docs()

        logger.info("Building download list")
        dl_list = self.build_download_list()

        logger.info("Running download and extract")
        dl = self.run_tasks(self.download, dl_list)
        self.log_run(dl)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "sedac_cookie": config["main"]["sedac_cookie"],
        "unzip_only": config["main"].getboolean("unzip_only"),
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_extract": config["main"].getboolean("overwrite_extract"),
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


    class_instance = GPWv4(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["sedac_cookie"], config_dict["unzip_only"], config_dict["overwrite_download"], config_dict["overwrite_extract"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
