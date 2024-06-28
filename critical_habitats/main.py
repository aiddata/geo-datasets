

import os
import requests
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
from typing import List, Literal
import zipfile
import shutil

from data_manager import Dataset


class CRHAB(Dataset):

    name = "Critical Habitats"

    def __init__(self,
                 raw_dir: str,
                 output_dir: str,
                 download_url: str,
                 max_retries: int,
                 overwrite_download: bool,
                 overwrite_output: bool):

        self.download_url = download_url
        self.download_path = Path(download_url)
        self.version = str(self.download_path.stem.split("_")[-1])

        self.raw_dir = Path(raw_dir) / self.version
        self.zip_path = self.raw_dir / self.download_path.name
        self.data_path = self.raw_dir / self.download_path.stem / "01_Data" / "Basic_Critical_Habitat_Raster.tif"
        self.output_dir = Path(output_dir) / self.version
        self.output_path = self.output_dir / "critical_habitats.tif"

        self.max_retries = max_retries

        self.overwrite_download = overwrite_download
        self.overwrite_output = overwrite_output


    def download_data(self):
        """
        Download data zip from source
        """
        logger = self.get_logger()

        if self.zip_path.exists() and not self.overwrite_download:
            logger.info(f"Download Exists: {self.zip_path}")
            return

        attempts = 1
        while attempts <= self.max_retries:
            try:
                with requests.get(self.download_url, stream=True, verify=True) as r:
                    r.raise_for_status()
                    with open(self.zip_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024*1024):
                            f.write(chunk)
                logger.info(f"Downloaded: {self.download_url}")
                return (self.download_url, self.zip_path)
            except Exception as e:
                attempts += 1
                if attempts > self.max_retries:
                    logger.info(f"{str(e)}: Failed to download: {str(self.download_url)}")
                    logger.exception(e)
                    raise
                else:
                    logger.info(f"Attempt {str(attempts)} : {str(self.download_url)}")


    def extract_data(self):
        """Extract data from downloaded zip file
        """

        logger = self.get_logger()

        if self.data_path.exists() and not self.overwrite_download:
            logger.info(f"Extract Exists: {self.zip_path}")
        elif not self.zip_path.exists():
            logger.info(f"Error: Data download not found: {self.zip_path}" )
            raise Exception(f"Data file not found: {self.zip_path}")
        else:
            logger.info(f"Extracting: {self.zip_path}")
            # extract zipfile to raw_dir
            with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.raw_dir)


    def process_data(self):
        """Copy extract file to output
        """
        logger = self.get_logger()

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.output_path.exists() and not self.overwrite_output:
            logger.info(f"Output Exists: {self.output_path}")
            return
        else:
            logger.info(f"Processing: {self.data_path}")
            shutil.copy(self.data_path, self.output_path)


    def main(self):

        logger = self.get_logger()

        logger.info("Running data download")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.download_data()

        logger.info("Extracting Data")
        self.extract_data()

        logger.info("Processing Data")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.process_data()


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "download_url": config["main"]["download_url"],
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

    class_instance = CRHAB(config_dict["raw_dir"], config_dict["output_dir"], config_dict["download_url"], config_dict["max_retries"], config_dict["overwrite_download"], config_dict["overwrite_output"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])


else:
    try:
        from prefect import flow
    except:
        pass
    else:
        config_file = "critical_habitats/config.ini"
        config = ConfigParser()
        config.read(config_file)

        from main import CRHAB

        tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

        @flow
        def crhab(
            raw_dir: str,
            output_dir: str,
            download_url: str,
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


            class_instance = CRHAB(raw_dir=raw_dir, output_dir=output_dir, download_url=download_url, max_retries=max_retries, overwrite_download=overwrite_download, overwrite_output=overwrite_output)


            if task_runner != 'hpc':
                os.chdir(tmp_dir)
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir)
            else:
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=hpc_cluster_kwargs)
