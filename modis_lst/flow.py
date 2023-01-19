import os
import sys
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub


config_file = "modis_lst/config.ini"
config = ConfigParser()
config.read(config_file)

block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')

from main import MODISLandSurfaceTemp

tmp_dir = Path(os.getcwd()) / config["github"]["directory"]


@flow
def modis_lst(raw_dir, output_dir, username, password, years, overwrite_download, overwrite_processing, backend, task_runner, run_parallel, max_workers, log_dir):

    timestamp = datetime.today()
    time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)

    cluster = "vortex"

    cluster_kwargs = {
        "shebang": "#!/bin/tcsh",
        "resource_spec": "nodes=1:c18a:ppn=12",
        "cores": 4,
        "processes": 4,
        "memory": "32GB",
        "interface": "ib0",
        "job_extra_directives": [
            "-j oe",
        ],
        "job_script_prologue": [
            f"cd {tmp_dir}",
        ],
        "log_directory": str(timestamp_log_dir),
    }


    class_instance = MODISLandSurfaceTemp(raw_dir=raw_dir, output_dir=output_dir, username=username, password=password, years=years, overwrite_download=overwrite_download, overwrite_processing=overwrite_processing)

    if task_runner != 'hpc':
        os.chdir(tmp_dir)
        class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir)
    else:
        class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=cluster_kwargs)
