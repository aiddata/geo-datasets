# data download for viirs nighttime lights data
# data from: https://eogdata.mines.edu/nighttime_light/

import os
import sys
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
from typing import List, Literal
import requests
import json
import gzip
import shutil
import subprocess

import numpy as np
import rasterio

from data_manager import Dataset


class VIIRS_NTL(Dataset):

    name = "VIIRS_NTL"

    def __init__(self, raw_dir, output_dir, months, years, username, password, client_secret, max_retries, cf_minimum, run_annual=True, run_monthly=True, overwrite_download=False, overwrite_extract=False, overwrite_processing=False):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.months = months
        self.years = years
        self.username = username
        self.password = password
        self.client_secret = client_secret
        self.max_retries = max_retries
        self.cf_minimum = cf_minimum
        self.run_annual = run_annual
        self.run_monthly = run_monthly
        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract
        self.overwrite_processing = overwrite_processing

        self.base_url = "https://eogdata.mines.edu/nighttime_light"

        # annual (non-tiled)
        self.annual_url_path = "annual/v21"

        # monthly (tiled)
        self.monthly_url_path = "monthly/v10"

        # monthly (non-tiled)
        self.monthly_notile_url_path = "monthly_notile/v10"


    def test_connection(self):
        # test connection
        test_request = requests.get(self.base_url, verify=True)
        test_request.raise_for_status()


    def get_token(self):
        """
        retrieves access token for data download
        """
        params = {
            'client_id' : 'eogdata_oidc',
            'client_secret' : self.client_secret,
            'username' : self.username,
            'password' : self.password,
            'grant_type' : 'password'
        }

        token_url = 'https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token'
        response = requests.post(token_url, data = params)
        access_token_dict = json.loads(response.text)
        access_token = access_token_dict.get('access_token')

        self.token = access_token


    def build_download_list(self):
        """
        Note: tiled monthly data is included and commented out as an artifact of previous versions but no plans to use it again
        """
        task_list = []
        logger = self.get_logger()

        task_list = []

        def run_cmd(cmd, retry=5):
            """
            Run a shell command via subprocess and return the output

            Note that wget will already output to stderr rather than stdout in this case
            """
            c = 1
            while c <= retry:
                try:
                    x = subprocess.run(cmd, shell=True, check=True, capture_output=True)
                    return str(x.stderr)
                except subprocess.CalledProcessError as e:
                    c += 1
                    if c > retry:
                        raise
                    else:
                        logger.warning(f'Error running command: {cmd}, retrying {c} of {retry}')
                        logger.warning(f'Error: {e}')
                        continue


        def get_links_from_wget(output, end_match_list):
            all_lines = output.split('\\n')
            file_lines = [i for i in all_lines if i.endswith(tuple([f'{i} 200 OK' for i in end_match_list]))]
            match_files = lambda x: [i for i in x.split(' ') if i.endswith(tuple(end_match_list))][0]
            file_links = [match_files(i) for i in file_lines]
            return file_links


        if self.run_annual:
            annual_links = []
            for y in self.years:
                data_url_path = f'{self.annual_url_path}/{y}'
                cmd_str = f'wget -e robots=off -m -np -nH --cut-dirs=1 --header "Authorization: Bearer {self.token}" -P "{self.raw_dir}"/download "{self.base_url}/{data_url_path}/" -R .html -A .tif.gz --spider -nv'
                x = run_cmd(cmd_str, self.max_retries)
                links = get_links_from_wget(x, ['.tif.gz'])
                annual_links.append((y, data_url_path, links))

            for i in annual_links:
                for src_url in i[2]:
                    dst_path = self.raw_dir / 'download' /  i[1] / Path(src_url).name
                    task_list.append([src_url, dst_path])


        # if self.run_monthly_tiled:
        #     monthly_links = []
        #     for y in self.years:
        #         for m in self.months:
        #             data_url_path = f'{self.monthly_url_path}/{y}/{y}{m}/vcmcfg'
        #             cmd_str = f'wget -e robots=off -m -np -nH --cut-dirs=1 --header "Authorization: Bearer {self.token}" -P "{self.raw_dir}"/download "{self.base_url}/{data_url_path}/" -R .html -A .tgz --spider -nv'
        #             x = run_cmd(cmd_str, self.max_retries)
        #             links = get_links_from_wget(x, ['.tgz'])
        #             monthly_links.append((y, m, data_url_path, links))

        #     for i in monthly_links:
        #         for src_url in i[3]:
        #             dst_path = self.raw_dir / 'download' /  i[2] / Path(src_url).name
        #             task_list.append([src_url, dst_path])


        if self.run_monthly:
            monthly_notile_links = []
            for y in self.years:
                for m in self.months:
                    m = f'{m:02d}'
                    data_url_path = f'{self.monthly_notile_url_path}/{y}/{y}{m}/vcmcfg'
                    cmd_str = f'wget -e robots=off -m -np -nH --cut-dirs=1 --header "Authorization: Bearer {self.token}" -P "{self.raw_dir}"/download "{self.base_url}/{data_url_path}/" -R .html -A .avg_rade9h.masked.tif,.cf_cvg.tif,.cvg.tif --spider -nv'
                    x = run_cmd(cmd_str, self.max_retries)
                    links = get_links_from_wget(x, ['.avg_rade9h.masked.tif', '.cf_cvg.tif', '.cvg.tif'])
                    monthly_notile_links.append((y, m, data_url_path, links))

            for i in monthly_notile_links:
                for src_url in i[3]:
                    dst_path = self.raw_dir / 'download' / i[2] / Path(src_url).name
                    task_list.append([src_url, dst_path])


        return task_list


    def download(self, src_url, dst_path):
        # consider doing separate directories for years when doing monthly data download
        """
        Download individual file
        """
        logger = self.get_logger()

        headers = {
            "Authorization": f"Bearer {self.token}",
        }

        if dst_path.exists() and not self.overwrite_download:
            logger.info(f"Download Exists: {dst_path}")
        else:
            try:
                with requests.get(src_url, headers=headers, stream=True) as src:
                    # raise an exception (fail this task) if HTTP response indicates that an error occured
                    src.raise_for_status()
                    with open(dst_path, "wb") as dst:
                        for chunk in src.iter_content(chunk_size=8192):
                            dst.write(chunk)

            except Exception:
                logger.exception(f"Failed to download: {str(src_url)}")
                raise
            else:
                logger.info(f"Downloaded {str(dst_path)}")

        return (src_url, dst_path)


    def build_extract_list(self):
        """
        Note: monthly_notile data is not gzipped so does not need to be extracted
        """
        task_list = []
        logger = self.get_logger()

        if self.run_annual:

            dl_gz_dir = self.raw_dir / "download" / self.annual_url_path
            all_dl_gz_files = list(dl_gz_dir.rglob("*.gz"))
            match_exts = (".average_masked.dat.tif.gz", ".cf_cvg.dat.tif.gz", ".cvg.dat.tif.gz")
            extract_files = [i for i in all_dl_gz_files if i.name.endswith(match_exts)]

            for extract_src in extract_files:
                extract_dst = Path(str(extract_src).replace(".gz", "").replace("/download/", "/extract/"))
                task_list.append((extract_src, extract_dst))


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
            output_filename.parent.mkdir(parents=True, exist_ok=True)
            try:
                with gzip.open(raw_local_filename, 'rb') as f_in:
                    with open(output_filename, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                return (raw_local_filename, output_filename)
            except Exception as e:
                logger.exception(f"Failed to extract: {str(raw_local_filename)}")


    def build_process_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            for year in self.years:
                avg_extract_path = list((self.raw_dir / "extract" / self.annual_url_path / str(year)).rglob("*.average_masked.dat.tif"))[0]
                avg_output_path = self.output_dir / self.annual_url_path / "averaged_masked" / f"{year}.tif"
                if avg_extract_path.exists():
                    task_list.append((avg_extract_path, avg_output_path))
                else:
                    logger.info(f"Failed to find extracted raw file: {str(avg_extract_path)}")

                cloud_extract_path = list((self.raw_dir / "extract" / self.annual_url_path / str(year)).rglob("*.cf_cvg.dat.tif"))[0]
                cloud_output_path = self.output_dir / self.annual_url_path / "cf_cvg" / f"{year}.tif"
                if cloud_extract_path.exists():
                    task_list.append((cloud_extract_path, cloud_output_path))
                else:
                    logger.info(f"Failed to find extracted raw file: {str(cloud_extract_path)}")


        if self.run_monthly:
            for year in self.years:
                for month in self.months:
                    format_month = str(month).zfill(2)

                    avg_extract_path = list((self.raw_dir / "download" / self.monthly_notile_url_path / str(year) / f"{year}{format_month}" / "vcmcfg").rglob("*.avg_rade9h.masked.tif"))[0]
                    avg_output_path = self.output_dir / self.monthly_notile_url_path / "averaged_masked" / f"{year}_{format_month}.tif"
                    if avg_extract_path.exists():
                        task_list.append((avg_extract_path, avg_output_path))
                    else:
                        logger.info(f"Failed to find extracted raw file: {str(avg_extract_path)}")

                    cloud_extract_path = list((self.raw_dir / "download" / self.monthly_notile_url_path / str(year) / f"{year}{format_month}" / "vcmcfg").rglob("*.cf_cvg.tif"))[0]
                    cloud_output_path = self.output_dir / self.monthly_notile_url_path / "cf_cvg" / f"{year}_{format_month}.tif"
                    if cloud_extract_path.exists():
                        task_list.append((cloud_extract_path, cloud_output_path))
                    else:
                        logger.info(f"Failed to find extracted raw file: {str(cloud_extract_path)}")


        logger.debug(f"{str(task_list)}")
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

        output_dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            if "cf_cvg" in str(raw_file):
                self.raster_calc(raw_file, output_dst, self.make_binary)
            else:
                self.raster_calc(raw_file, output_dst, self.remove_negative)
        except Exception:
            logger.exception(f"Failed to process: {str(raw_file)}")
        else:
            logger.info(f"File Processed: {str(output_dst)}")
            return (raw_file, output_dst)


    def main(self):
        logger = self.get_logger()

        logger.info("Retrieving token")
        self.get_token()

        logger.info("Testing Connection...")
        self.test_connection()

        os.makedirs(self.raw_dir, exist_ok=True)
        logger.info("Building download list...")
        self.dl_list = self.build_download_list()

        logger.info(self.dl_list)

        if self.dl_list:
            logger.info("Running data download")
            dl = self.run_tasks(self.download, self.dl_list)
            self.log_run(dl)
        else:
            logger.info("No files to download")

        logger.info("Building extract list...")
        extract_list = self.build_extract_list()

        logger.info(extract_list)

        logger.info("Extracting raw files")
        extraction = self.run_tasks(self.extract_files, extract_list)
        self.log_run(extraction)


        logger.info("Building processing list...")
        process_list = self.build_process_list()

        logger.info(process_list)

        logger.info("Processing raw files")
        process = self.run_tasks(self.process_files, process_list)
        self.log_run(process)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "run_annual" :  config["main"].getboolean("run_annual"),
            "run_monthly" :  config["main"].getboolean("run_monthly"),
            "years": [int(y) for y in config["main"]["years"].split(", ")],
            "months": [int(y) for y in config["main"]["months"].split(", ")],
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "log_dir": Path(config["main"]["raw_dir"]) / "logs",
            "overwrite_download": config["main"].getboolean("overwrite_download"),
            "overwrite_extract": config["main"].getboolean("overwrite_extract"),
            "overwrite_processing": config["main"].getboolean("overwrite_processing"),
            "max_retries": config["main"].getint("max_retries"),
            "cf_minimum": config["main"].getint("cf_minimum"),

            "username" : config["download"]["username"],
            "password" : config["download"]["password"],
            "client_secret" : config["download"]["client_secret"],

            "backend": config["run"]["backend"],
            "task_runner": config["run"]["task_runner"],
            "run_parallel": config["run"].getboolean("run_parallel"),
            "max_workers": int(config["run"]["max_workers"])
        }

if __name__ == "__main__":
    config_dict = get_config_dict()

    class_instance = VIIRS_NTL(config_dict["raw_dir"], config_dict["output_dir"], config_dict["months"], config_dict["years"], config_dict["username"], config_dict["password"], config_dict["client_secret"], config_dict["max_retries"], config_dict["cf_minimum"], config_dict["run_annual"], config_dict["run_monthly"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])

else:
    try:
        from prefect import flow
        from prefect.filesystems import GitHub
    except:
        pass
    else:
        config_file = "viirs_ntl/config.ini"
        config = ConfigParser()
        config.read(config_file)

        block_name = config["deploy"]["storage_block"]
        tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

        @flow
        def viirs_ntl(
            raw_dir: str,
            output_dir: str,
            months: List[int],
            years: List[int],
            username: str,
            password: str,
            client_secret: str,
            max_retries: int,
            cf_minimum: int,
            run_annual: bool,
            run_monthly: bool,
            overwrite_download: bool,
            overwrite_extract: bool,
            overwrite_processing: bool,
            backend: Literal["local", "mpi", "prefect"],
            task_runner: Literal["sequential", "concurrent", "dask", "hpc", "kubernetes"],
            run_parallel: bool,
            max_workers: int,
            log_dir: str):

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
                "log_directory": str(timestamp_log_dir)
            }

            class_instance = VIIRS_NTL(raw_dir, output_dir, months, years, username, password, client_secret, max_retries, cf_minimum, run_annual, run_monthly, overwrite_download, overwrite_extract, overwrite_processing)

            if task_runner != 'hpc':
                os.chdir(tmp_dir)
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir)
            else:
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=cluster_kwargs)
