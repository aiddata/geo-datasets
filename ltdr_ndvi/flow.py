import os
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser
from typing import List, Literal

from prefect import flow
from prefect.filesystems import GitHub


config_file = "ltdr_ndvi/config.ini"
config = ConfigParser()
config.read(config_file)

block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')

from main import LTDR_NDVI

tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

@flow
def ltdr_ndvi(
        token: str,
        years: List[int],
        raw_dir: str,
        output_dir: str,
        overwrite_download: bool,
        validate_download: bool,
        overwrite_processing: bool,
        backend: Literal["local", "mpi", "prefect"],
        task_runner: Literal["sequential", "concurrent", "dask", "hpc"],
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


    # cluster = "hima"

    # cluster_kwargs = {
    #     "shebang": "#!/bin/tcsh",
    #     "resource_spec": "nodes=1:hima:ppn=32",
    #     "cores": 2,
    #     "processes": 2,
    #     "memory": "30GB",
    #     "interface": "ib0",
    #     "job_extra_directives": [
    #         "#PBS -j oe",
    #         # "#PBS -o ",
    #         # "#PBS -e ",
    #     ],
    #     "job_script_prologue": [
    #         "source /usr/local/anaconda3-2020.02/etc/profile.d/conda.csh",
    #         "module load anaconda3/2021.05",
    #         "conda activate geodata_38h1",
    #         f"cd {tmp_dir}",
    #     ],
    #     "log_directory": str(timestamp_log_dir)
    # }

    class_instance = LTDR_NDVI(token, years, raw_dir, output_dir, overwrite_download, validate_download, overwrite_processing)

    if task_runner != 'hpc':
        os.chdir(tmp_dir)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=cluster_kwargs)
