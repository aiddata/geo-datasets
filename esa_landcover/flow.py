import os
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub


config_file = "esa_landcover/config.ini"
config = ConfigParser()
config.read(config_file)

block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')

from main import ESALandcover

tmp_dir = Path(os.getcwd()) / config["github"]["directory"]


@flow
def esa_landcover(raw_dir, output_dir, years, overwrite_download, overwrite_processing, backend, task_runner, run_parallel,  max_workers, log_dir):

    timestamp = datetime.today()
    time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)

    cluster = "vortex"

    cluster_kwargs = {
        "shebang": "#!/bin/tcsh",
        "resource_spec": "nodes=1:c18a:ppn=12",
        "cores": 2,
        "processes": 2,
        "memory": "15GB",
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

    class_instance = ESALandcover(raw_dir, output_dir, years, overwrite_download, overwrite_processing)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=cluster_kwargs)
