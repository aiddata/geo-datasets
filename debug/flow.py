

import sys, os
from configparser import ConfigParser

from prefect import flow
from prefect.filesystems import GitHub


config_file = "debug/config.ini"
config = ConfigParser()
config.read(config_file)


block_name = config["deploy"]["storage_block"]
GitHub.load(block_name).get_directory('global_scripts')
GitHub.load(block_name).get_directory(config["github"]["directory"])

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), config["github"]["directory"]))


from main_combo import DebugDataset


@flow
def malaria_atlas_project(raw_dir, backend, task_runner, run_parallel, max_workers, log_dir):
    import sys, os
    sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), config["github"]["directory"]))
    print(os.listdir(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), config["github"]["directory"])))
    print(sys.path)

    from main_combo import DebugDataset

    class_instance = DebugDataset(raw_dir)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=log_dir)
