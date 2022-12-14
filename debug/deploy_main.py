
import os
import sys
import requests
from pathlib import Path
from configparser import ConfigParser


#sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
#sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')

from dataset import Dataset


class DebugDataset(Dataset):
    name = "Debug Dataset"

    def __init__(self, raw_dir):

        #sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')
        self.raw_dir = Path(raw_dir)


    def test_connection(self):
        # test connection
        test_request = requests.get("https://data.malariaatlas.org", verify=True)
        test_request.raise_for_status()


    def test_task(self, x):
        """download individual file using session created
        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """
        logger = self.get_logger()
        print('md', sys.path)
        #sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')
        sys.path.insert(1, 'geo-datasets/global_scripts')
        sys.path.insert(1, 'geo-datasets/malaria_atlas_project')
        
        logger.info(f"Job Task: {x}")
        print(x)
        return x * 10
       

    def main(self):

        logger = self.get_logger()

        raw_zip_dir = self.raw_dir
        raw_zip_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Testing Connection...")
        self.test_connection()

        #sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')
        sys.path.insert(1, 'geo-datasets/global_scripts')
        sys.path.insert(1, 'geo-datasets/debug')

        logger.info("Running test task")
        # download data zipFile from url to the local output directory
        tests = self.run_tasks(self.test_task, [1,2,3,4,5])
        self.log_run(tests)



def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs"
    }

