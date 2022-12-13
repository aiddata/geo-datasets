import sys, os
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub


config_file = "malaria_atlas_project/config.ini"
config = ConfigParser()
config.read(config_file)


block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')

# sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), config["github"]["directory"]))


from malaria import MalariaAtlasProject


@flow
def malaria_atlas_project(raw_dir, output_dir, years, dataset, overwrite_download, overwrite_processing, backend, task_runner, run_parallel, max_workers, log_dir):
    sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), config["github"]["directory"]))

    print(sys.path)

    from malaria import MalariaAtlasProject


    class_instance = MalariaAtlasProject(raw_dir, output_dir, years, dataset, overwrite_download, overwrite_processing)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=log_dir)
