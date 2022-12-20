import os
import sys
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub

config_file = "dvnl/config.ini"
config = ConfigParser()
config.read(config_file)

block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')

from main import DVNL

tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

@flow
def dvnl(raw_dir, output_dir, years, overwrite_download, overwrite_processing, backend, task_runner, run_parallel, max_workers, cores_per_process, log_dir):

    timestamp = datetime.today()
    time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)


    cluster_kwargs = {
        "shebang": "#!/bin/tcsh",
        "resource_spec": "nodes=1:c18a:ppn=12",
        "cores": 12,
        "processes": 1,
        "memory": "30GB",
        "interface": "ib0",
        "job_extra_directives": [
            "#PBS -j oe",
            # "#PBS -o ",
            # "#PBS -e ",
        ],
        "job_script_prologue": [
            f"cd {tmp_dir}",
        ],
        "log_directory": str(timestamp_log_dir)
    }

    class_instance = DVNL(raw_dir, output_dir, years, overwrite_download, overwrite_processing)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, cores_per_process=cores_per_process, log_dir=timestamp_log_dir, cluster_kwargs=cluster_kwargs)