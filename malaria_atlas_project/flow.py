import os
import sys
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub

from main import MalariaAtlasProject


config_file = "malaria_atlas_project/config.ini"
config = ConfigParser()
config.read(config_file)

block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')


# sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), config["github"]["directory"]))
tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

@flow
def malaria_atlas_project(raw_dir, output_dir, years, dataset, overwrite_download, overwrite_processing, backend, task_runner, run_parallel, max_workers, cores_per_process, log_dir):

    timestamp = datetime.today()
    time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = Path(os.getcwd()) / 'malaria_atlas_project'

    cluster_kwargs = {
        "shebang": "#!/bin/tcsh",
        "resource_spec": "nodes=1:c18a:ppn=12",
        "cores": 12,
        "processes": 12,
        "memory": "30GB",
        "interface": "ib0",
        "job_extra_directives": [
            "#PBS -j oe",
            # "#PBS -o ",
            # "#PBS -e ",
        ],

        # "job_script_prologue": [
        #     f"cd {tmp_dir}",
        # ],

        "job_script_prologue": [
            "source /usr/local/anaconda3-2021.05/etc/profile.d/conda.csh",
            "module load gcc/9.3.0 openmpi/3.1.4/gcc-9.3.0 anaconda3/2021.05",
            "conda activate geodata38",
            "set tmpdir=`mktemp -d`",
            "cd $tmpdir",
            "git clone -b develop https://github.com/aiddata/geo-datasets.git",
            "cd geo-datasets/malaria_atlas_project",
            "cp ../global_scripts/* ."
        ],

        "log_directory": str(timestamp_log_dir)
    }

    class_instance = MalariaAtlasProject(raw_dir, output_dir, years, dataset, overwrite_download, overwrite_processing)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, cores_per_process=cores_per_process, log_dir=timestamp_log_dir, cluster_kwargs=cluster_kwargs)
