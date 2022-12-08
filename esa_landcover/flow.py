import sys, os
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub


config_file = "esa_landcover/config.ini"
config = ConfigParser()
config.read(config_file)


block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')

# sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), config["github"]["directory"]))


from main import ESALandcover


@flow
def esa_landcover(raw_dir, output_dir, years, overwrite_download, overwrite_processing, backend, task_runner, run_parallel,  max_workers, log_dir):

    class_instance = ESALandcover(raw_dir, output_dir, years, overwrite_download, overwrite_processing)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=log_dir)
