# data download script for PLAD political leaders' birthplace dataset
# info link: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YUS575

import os
import requests
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
from typing import List, Literal

import pandas as pd
import rasterio
from rasterio import features
from shapely.geometry import Point
import geopandas as gpd

from data_manager import Dataset


class PLAD(Dataset):

    name = "PLAD"

    def __init__(self,
                 raw_dir: str,
                 output_dir: str,
                 years: list,
                 max_retries: int,
                 overwrite_download: bool,
                 overwrite_output: bool):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.max_retries = max_retries
        self.overwrite_download = overwrite_download
        self.overwrite_output = overwrite_output
        self.dataset_url = "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YUS575"
        self.download_url = "https://dataverse.harvard.edu/api/access/datafile/10119325?format=tab&gbrecs=true"

        self.src_path = self.raw_dir / "plad.tab"

    def test_connection(self):
        # test connection
        test_request = requests.get(self.dataset_url, verify=True)
        test_request.raise_for_status()

    def download_data(self):
        """
        Download original spreadsheet
        """
        logger = self.get_logger()

        if os.path.isfile(self.src_path) and not self.overwrite_download:
            logger.info(f"Download Exists: {self.src_path}")
        else:
            attempts = 1
            while attempts <= self.max_retries:
                try:
                    with requests.get(self.download_url, stream=True, verify=True) as r:
                        r.raise_for_status()
                        with open(self.src_path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024):
                                f.write(chunk)
                    logger.info(f"Downloaded: {self.download_url}")
                    return (self.download_url, self.src_path)
                except Exception as e:
                    attempts += 1
                    if attempts > self.max_retries:
                        logger.info(f"{str(e)}: Failed to download: {str(self.download_url)}")
                        return (self.download_url, self.src_path)
                    else:
                        logger.info(f"Attempt {str(attempts)} : {str(self.download_url)}")

    def process_year(self, year):
        """create file for each year
        """

        logger = self.get_logger()

        output_filename = f"leader_birthplace_data_{year}.tif"
        output_path = self.output_dir / output_filename

        if os.path.isfile(output_path) and not self.overwrite_output:
            logger.info(f"File exists: {str(output_path)}")
            return ("File exists", str(output_path))


        if not os.path.isfile(self.src_path):
            logger.info(f"Error: Master data download: {self.src_path} not found" )
            raise Exception(f"Data file not found: {self.src_path}")

        src_df = pd.read_csv(self.src_path, sep="\t", low_memory=False)

        # adm2 or finer precision
        df = src_df.loc[src_df.geo_precision.isin([1,2,3])].copy()

        df = df.loc[(df.startyear <= year) & (df.endyear >= year)].copy()


        df["geometry"] = df.apply(lambda x: Point(x.longitude, x.latitude), axis=1)

        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        gdf = gdf.set_crs(epsg=4326)

        pixel_size = 0.05
        transform = rasterio.transform.from_origin(-180, 90, pixel_size, pixel_size)
        shape = (int(180/pixel_size), int(360/pixel_size))

        rasterized = features.rasterize(gdf.geometry,
                                        out_shape = shape,
                                        fill = 0,
                                        out = None,
                                        transform = transform,
                                        all_touched = True,
                                        default_value = 1,
                                        dtype = None)

        with rasterio.open(
                output_path, "w",
                driver = "GTiff",
                crs = "EPSG:4326",
                transform = transform,
                dtype = rasterio.uint8,
                count = 1,
                width = shape[1],
                height = shape[0]) as dst:
            dst.write(rasterized, indexes = 1)

        logger.info(f"Data Compiled: {str(year)}")


    def main(self):

        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        logger.info("Running data download")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.download_data()

        logger.info("Sorting Data")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        sort = self.run_tasks(self.process_year, [[y,] for y in self.years])
        self.log_run(sort)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "years": [int(y) for y in config["main"]["years"].split(", ")],
            "log_dir": Path(config["main"]["output_dir"]) / "logs",
            "backend": config["run"]["backend"],
            "task_runner": config["run"]["task_runner"],
            "run_parallel": config["run"].getboolean("run_parallel"),
            "max_workers": int(config["run"]["max_workers"]),
            "max_retries": config["main"].getint("max_retries"),
            "overwrite_download": config["main"].getboolean("overwrite_download"),
            "overwrite_output": config["main"].getboolean("overwrite_output"),
        }


if __name__ == "__main__":

    config_dict = get_config_dict()

    log_dir = config_dict["log_dir"]
    timestamp = datetime.today()
    time_format_str: str="%Y_%m_%d_%H_%M"
    time_str = timestamp.strftime(time_format_str)
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)

    class_instance = PLAD(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["max_retries"], config_dict["overwrite_download"], config_dict["overwrite_output"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])


else:
    try:
        from prefect import flow
    except:
        pass
    else:
        config_file = "plad/config.ini"
        config = ConfigParser()
        config.read(config_file)

        from main import PLAD

        tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

        @flow
        def plad(
            raw_dir: str,
            output_dir: str,
            years: List[int],
            max_retries: int,
            overwrite_download: bool,
            overwrite_output: bool,
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


            hpc_cluster_kwargs = {
                "shebang": "#!/bin/tcsh",
                "resource_spec": "nodes=1:c18a:ppn=12",
                "walltime": "4:00:00",
                "cores": 3,
                "processes": 3,
                "memory": "30GB",
                "interface": "ib0",
                "job_extra_directives": [
                    "-j oe",
                ],
                "job_script_prologue": [
                    "source /usr/local/anaconda3-2021.05/etc/profile.d/conda.csh",
                    "module load anaconda3/2021.05",
                    "conda activate geodata38",
                    f"cd {tmp_dir}",
                ],
                "log_directory": str(timestamp_log_dir),
            }


            class_instance = PLAD(raw_dir=raw_dir, output_dir=output_dir, years=years, max_retries=max_retries, overwrite_download=overwrite_download, overwrite_output=overwrite_output)


            if task_runner != 'hpc':
                os.chdir(tmp_dir)
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir)
            else:
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=hpc_cluster_kwargs)
