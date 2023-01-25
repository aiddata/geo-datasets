import os
import sys
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub


config_file = "gpw/config.ini"
config = ConfigParser()
config.read(config_file)

block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory("global_scripts")

from main import GPWv4

tmp_dir = Path(os.getcwd()) / config["github"]["directory"]


@flow
def gpwv4(raw_dir, output_dir, years, sedac_cookie, unzip_only, overwrite_download, overwrite_extract, backend, task_runner, run_parallel, max_workers, log_dir):

    timestamp = datetime.today()
    time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)

    cluster = "vortex"

    cluster_kwargs = {
        "shebang": "#!/bin/tcsh",
        "resource_spec": "nodes=1:c18a:ppn=12",
        "walltime": "2:00:00",
        "cores": 10,
        "processes": 10,
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


    class_instance = GPWv4(raw_dir, output_dir, years, sedac_cookie, unzip_only, overwrite_download, overwrite_extract)

    if task_runner != 'hpc':
        os.chdir(tmp_dir)
        class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir)
    else:
        class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=cluster_kwargs)
