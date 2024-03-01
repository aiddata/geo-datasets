# data download for viirs nighttime lights data
# data from: https://eogdata.mines.edu/nighttime_light/

import gzip
import json
import os
import shutil
import sys
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import List, Literal

import numpy as np
import rasterio
import requests
from bs4 import BeautifulSoup
from data_manager import Dataset

sys.path.insert(
    1,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "global_scripts"
    ),
)


class VIIRS_NTL(Dataset):
    name = "VIIRS_NTL"

    def __init__(
        self,
        raw_dir,
        output_dir,
        run_annual,
        annual_files,
        run_monthly,
        monthly_files,
        months,
        years,
        username,
        password,
        client_secret,
        max_retries,
        cf_minimum,
        overwrite_download=False,
        overwrite_extract=False,
        overwrite_processing=False,
    ):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.run_annual = run_annual
        self.annual_files = annual_files
        self.run_monthly = run_monthly
        self.monthly_files = monthly_files
        self.months = months
        self.years = years
        self.username = username
        self.password = password
        self.client_secret = client_secret
        self.max_retries = max_retries
        self.cf_minimum = cf_minimum
        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract
        self.overwrite_processing = overwrite_processing

    def test_connection(self):
        # test connection
        test_request = requests.get(
            "https://eogdata.mines.edu/nighttime_light/", verify=True
        )
        test_request.raise_for_status()

    def get_token(self):
        """
        retrieves access token for data download
        """
        params = {
            "client_id": "eogdata_oidc",
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }

        token_url = (
            "https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token"
        )
        response = requests.post(token_url, data=params)
        access_token_dict = json.loads(response.text)
        access_token = access_token_dict.get("access_token")

        return access_token

    def build_download_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            # TODO: pull from beautiful soup for file url, filter out non-available urls here
            for year in self.years:
                for file in self.annual_files:
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v21/{YEAR}/VNL_v21_npp_{YEAR}_global_{CONFIG}_c202205302300.{TYPE}.dat.tif.gz"
                    if int(year) < 2014:
                        file_config = "vcmcfg"
                    else:
                        file_config = "vcmslcfg"
                    download_dest = download_url.format(YEAR=year, TYPE=file, CONFIG=file_config)
                    local_filename = (
                        self.raw_dir / f"raw_viirs_ntl_{year}_{file}.tif.gz"
                    )
                    task_list.append((download_dest, local_filename))
        else:
            for year in self.years:
                for month in self.months:
                    for file in self.monthly_files:
                        if (year == 2022) & (month == 8):
                            download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/NOAA-20/vcmcfg/"
                        else:
                            download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmcfg/"

                        if int(month) < 10:
                            format_month = "0" + str(month)
                        else:
                            format_month = str(month)

                        download_url = download_url.format(
                            YEAR=year, MONTH=format_month
                        )

                        attempts = 1
                        while attempts <= self.max_retries:
                            try:
                                session = requests.Session()
                                r = session.get(
                                    download_url, headers={"User-Agent": "Mozilla/5.0"}
                                )
                                soup = BeautifulSoup(r.content, "html.parser")

                                items = soup.find_all("tr", {"class": "odd"})
                                link_list = []

                                for i in items:
                                    link = str(i.findChild("a")["href"])
                                    link_list.append(link)

                                file_type = file + ".tif.gz"
                                file_code = ""
                                for link in link_list:
                                    if file_type in link:
                                        file_code = link
                            except Exception as e:
                                attempts += 1
                                if attempts > self.max_retries:
                                    logger.info(
                                        str(e)
                                        + f": Failed to download: {str(download_dest)}"
                                    )
                                else:
                                    logger.info("Retrieved: " + str(download_dest))

                        if len(file_code) == 0:
                            logger.info(
                                "Download option does not exist yet: "
                                + str(year)
                                + "/"
                                + str(month)
                                + "/"
                                + file
                            )
                        else:
                            download_dest = download_url + str(file_code)
                            local_filename = (
                                self.raw_dir
                                / f"raw_viirs_ntl_{year}_{month}_{file}.tif.gz"
                            )
                            task_list.append((download_dest, local_filename))

        return task_list

    def manage_download(self, download_dest, local_filename):
        # consider doing separate directories for years when doing monthly data download
        """
        Download individual file
        """
        logger = self.get_logger()

        logger.info("Retrieving token")
        token = self.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
        }

        if local_filename.exists() and not self.overwrite_download:
            logger.info(f"Download Exists: {local_filename}")
        else:
            try:
                with requests.get(download_dest, headers=headers, stream=True) as src:
                    # raise an exception (fail this task) if HTTP response indicates that an error occured
                    src.raise_for_status()
                    with open(local_filename, "wb") as dst:
                        dst.write(src.content)
            except Exception as e:
                logger.info(f"Failed to download: {str(download_dest)}")
                raise Exception(str(e) + f": Failed to download: {str(download_dest)}")
            else:
                logger.info(f"Downloaded {str(local_filename)}")

        return (download_dest, local_filename)

    def build_extract_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            for year in self.years:
                for file in self.annual_files:
                    raw_local_filename = (
                        self.raw_dir / f"raw_viirs_ntl_{year}_{file}.tif.gz"
                    )
                    output_filename = (
                        self.output_dir / f"raw_extracted_viirs_ntl_{year}_{file}.tif"
                    )
                    if raw_local_filename.exists():
                        task_list.append((raw_local_filename, output_filename))
                    else:
                        logger.info(f"Raw file not located:  {str(raw_local_filename)}")
        if self.run_monthly:
            for year in self.years:
                for month in self.months:
                    for file in self.monthly_files:
                        if int(month) < 10:
                            format_month = "0" + str(month)
                        else:
                            format_month = month

                        raw_local_filename = (
                            self.raw_dir
                            / f"raw_viirs_ntl_{year}_{format_month}_{file}.tif.gz"
                        )
                        output_filename = (
                            self.output_dir
                            / f"raw_extracted_viirs_ntl_{year}_{format_month}_{file}.tif"
                        )
                        if raw_local_filename.exists():
                            task_list.append((raw_local_filename, output_filename))
                        else:
                            logger.info(
                                f"Raw file not located:  {str(raw_local_filename)}"
                            )

        return task_list

    def extract_files(self, raw_local_filename, output_filename):
        """
        Extract individual file
        """
        logger = self.get_logger()
        if output_filename.exists() and not self.overwrite_extract:
            logger.info(f"Extracted File Exists: {output_filename}")
            return (raw_local_filename, output_filename)
        else:
            try:
                with gzip.open(raw_local_filename, "rb") as f_in:
                    with open(output_filename, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                return (raw_local_filename, output_filename)
            except Exception as e:
                logger.info(f"Failed to extract: {str(raw_local_filename)}")
                raise Exception(
                    str(e) + ": " f"Failed to extract: {str(raw_local_filename)}"
                )

    def build_process_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            for year in self.years:
                annual_avg_glob_str = (
                    self.output_dir
                    / f"raw_extracted_viirs_ntl_{year}_average_masked.tif"
                )
                output_avg_glob = (
                    self.output_dir / f"viirs_ntl_annual_{year}_avg_masked.tif"
                )
                if annual_avg_glob_str.exists():
                    task_list.append((annual_avg_glob_str, output_avg_glob))
                else:
                    logger.info(
                        f"Failed to find extracted raw file: {str(annual_avg_glob_str)}"
                    )

                annual_cloud_glob_str = (
                    self.output_dir / f"raw_extracted_viirs_ntl_{year}_cf_cvg.tif"
                )
                output_cloud_glob = (
                    self.output_dir / f"viirs_ntl_annual_{year}_cf_cvg.tif"
                )
                if annual_cloud_glob_str.exists():
                    task_list.append((annual_cloud_glob_str, output_cloud_glob))
                else:
                    logger.info(
                        f"Failed to find extracted raw file: {str(annual_cloud_glob_str)}"
                    )

        if self.run_monthly:
            for year in self.years:
                for month in self.months:
                    if int(month) < 10:
                        format_month = "0" + str(month)
                    else:
                        format_month = month

                    monthly_avg_glob_str = (
                        self.output_dir
                        / f"raw_extracted_viirs_ntl_{year}_{format_month}_avg_rade9h.masked.tif"
                    )
                    output_avg_glob = (
                        self.output_dir
                        / f"viirs_ntl_monthly_{year}_{format_month}_avg_masked.tif"
                    )
                    if annual_avg_glob_str.exists():
                        task_list.append((monthly_avg_glob_str, output_avg_glob))
                    else:
                        logger.info(
                            f"Failed to find extracted raw file: {str(monthly_avg_glob_str)}"
                        )

                    monthly_cloud_glob_str = (
                        self.output_dir
                        / f"raw_extracted_viirs_ntl_{year}_{format_month}_cf_cvg.tif"
                    )
                    output_cloud_glob = (
                        self.output_dir / f"viirs_ntl_monthly_{year}_{month}_cf_cvg.tif"
                    )
                    if annual_cloud_glob_str.exists():
                        task_list.append((monthly_cloud_glob_str, output_cloud_glob))
                    else:
                        logger.info(
                            f"Failed to find extracted raw file: {str(monthly_cloud_glob_str)}"
                        )

        logger.info(f"{str(task_list)}")
        return task_list

    def raster_calc(self, input_path, output_path, function, **kwargs):
        """
        Calculate raster values using rasterio based on function provided

        :param input_path: input raster
        :param output_path: path to write output raster to
        :param function: function to apply to input raster values
        :param kwargs: additional meta args used to write output raster
        """
        with rasterio.Env(GDAL_CACHEMAX=100, CHECK_DISK_FREE_SPACE=False):
            # GDAL_CACHEMAX value in MB
            # https://trac.osgeo.org/gdal/wiki/ConfigOptions#GDAL_CACHEMAX
            # See: https://github.com/mapbox/rasterio/issues/1281
            with rasterio.open(input_path) as src:
                assert len(set(src.block_shapes)) == 1
                meta = src.meta.copy()
                meta.update(**kwargs)
                with rasterio.open(output_path, "w", **meta) as dst:
                    for ji, window in src.block_windows(1):
                        in_data = src.read(window=window)
                        out_data = function(in_data)
                        out_data = out_data.astype(meta["dtype"])
                        dst.write(out_data, window=window)

    def remove_negative(self, x):
        """
        remove negative values from array
        """
        return np.where(x > 0, x, 0)

    def make_binary(self, x):
        """
        create binary array based on threshold value
        """
        threshold = self.cf_minimum
        return np.where(x >= threshold, 1, 0)

    def process_files(self, raw_file, output_dst):
        logger = self.get_logger()
        if output_dst.exists() and not self.overwrite_processing:
            logger.info(f"Processed File Exists: {str(raw_file)}")
            return (raw_file, output_dst)
        try:
            if "cf_cvg" in str(raw_file):
                self.raster_calc(raw_file, output_dst, self.make_binary)
            else:
                self.raster_calc(raw_file, output_dst, self.remove_negative)
            logger.info(f"File Processed: {str(output_dst)}")
            return (raw_file, output_dst)
        except Exception as e:
            logger.info(f"Failed to process: {str(raw_file)}")
            raise Exception(str(e) + f": Failed to process: {str(raw_file)}")

    def main(self):
        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        os.makedirs(self.raw_dir, exist_ok=True)
        logger.info("Building download list...")
        dl_list = self.build_download_list()

        logger.info("Running data download")
        download = self.run_tasks(self.manage_download, dl_list)
        self.log_run(download)

        logger.info("Building extract list...")
        extract_list = self.build_extract_list()
        os.makedirs(self.output_dir, exist_ok=True)

        logger.info("Extracting raw files")
        extraction = self.run_tasks(self.extract_files, extract_list)
        self.log_run(extraction)

        logger.info("Building processing list...")
        process_list = self.build_process_list()

        logger.info("Processing raw files")
        process = self.run_tasks(self.process_files, process_list)
        self.log_run(process)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "run_annual": config["main"].getboolean("run_annual"),
        "run_monthly": config["main"].getboolean("run_monthly"),
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "months": [int(y) for y in config["main"]["months"].split(", ")],
        "annual_files": [str(y) for y in config["main"]["annual_files"].split(", ")],
        "monthly_files": [str(y) for y in config["main"]["monthly_files"].split(", ")],
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs",
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_extract": config["main"].getboolean("overwrite_extract"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "max_retries": config["main"].getint("max_retries"),
        "cf_minimum": config["main"].getint("cf_minimum"),
        "username": config["download"]["username"],
        "password": config["download"]["password"],
        "client_secret": config["download"]["client_secret"],
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
    }


if __name__ == "__main__":
    config_dict = get_config_dict()

    class_instance = VIIRS_NTL(
        raw_dir=config_dict["raw_dir"],
        output_dir=config_dict["output_dir"],
        run_annual=config_dict["run_annual"],
        annual_files=config_dict["annual_files"],
        run_monthly=config_dict["run_monthly"],
        monthly_files=config_dict["monthly_files"],
        months=config_dict["months"],
        years=config_dict["years"],
        username=config_dict["username"],
        password=config_dict["password"],
        client_secret=config_dict["client_secret"],
        max_retries=config_dict["max_retries"],
        cf_minimum=config_dict["cf_minimum"],
        overwrite_download=config_dict["overwrite_download"],
        overwrite_extract=config_dict["overwrite_extract"],
        overwrite_processing=config_dict["overwrite_processing"],
    )

    class_instance.run(
        backend=config_dict["backend"],
        run_parallel=config_dict["run_parallel"],
        max_workers=config_dict["max_workers"],
        task_runner=config_dict["task_runner"],
        log_dir=config_dict["log_dir"],
    )
else:
    try:
        from prefect import flow
    except ModuleNotFoundError:
        pass
    else:
        config_file = "viirs_ntl/config.ini"
        config = ConfigParser()
        config.read(config_file)

        tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

        @flow
        def viirs_ntl(
            raw_dir: str,
            output_dir: str,
            run_annual: bool,
            annual_files: List[str],
            run_monthly: bool,
            monthly_files: List[str],
            months: List[int],
            years: List[int],
            username: str,
            password: str,
            client_secret: str,
            max_retries: int,
            cf_minimum: int,
            overwrite_download: bool,
            overwrite_extract: bool,
            overwrite_processing: bool,
            backend: Literal["local", "mpi", "prefect"],
            task_runner: Literal["sequential", "concurrent", "dask", "hpc"],
            run_parallel: bool,
            max_workers: int,
            log_dir: str,
            bypass_error_wrapper: bool,
        ):
            timestamp = datetime.today()
            time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
            timestamp_log_dir = Path(log_dir) / time_str
            timestamp_log_dir.mkdir(parents=True, exist_ok=True)

            cluster = "vortex"

            cluster_kwargs = {
                "shebang": "#!/bin/tcsh",
                "resource_spec": "nodes=1:c18a:ppn=12",
                "cores": 6,
                "processes": 6,
                "memory": "32GB",
                "interface": "ib0",
                "job_extra_directives": [
                    "#PBS -j oe",
                    # "#PBS -o ",
                    # "#PBS -e ",
                ],
                "job_script_prologue": [
                    "source /usr/local/anaconda3-2021.05/etc/profile.d/conda.csh",
                    "module load anaconda3/2021.05",
                    "conda activate geodata38",
                    f"cd {tmp_dir}",
                ],
                "log_directory": str(timestamp_log_dir),
            }

            class_instance = VIIRS_NTL(
                raw_dir=raw_dir,
                output_dir=output_dir,
                run_annual=run_annual,
                annual_files=annual_files,
                run_monthly=run_monthly,
                monthly_files=monthly_files,
                months=months,
                years=years,
                username=username,
                password=password,
                client_secret=client_secret,
                max_retries=max_retries,
                cf_minimum=cf_minimum,
                overwrite_download=overwrite_download,
                overwrite_extract=overwrite_extract,
                overwrite_processing=overwrite_processing,
            )

            if task_runner != "hpc":
                os.chdir(tmp_dir)
                class_instance.run(
                    backend=backend,
                    task_runner=task_runner,
                    run_parallel=run_parallel,
                    max_workers=max_workers,
                    log_dir=timestamp_log_dir,
                    bypass_error_wrapper=bypass_error_wrapper,
                )
            else:
                class_instance.run(
                    backend=backend,
                    task_runner=task_runner,
                    run_parallel=run_parallel,
                    max_workers=max_workers,
                    log_dir=timestamp_log_dir,
                    cluster=cluster,
                    cluster_kwargs=cluster_kwargs,
                    bypass_error_wrapper=bypass_error_wrapper,
                )
