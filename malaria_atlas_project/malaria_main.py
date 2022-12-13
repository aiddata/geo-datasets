"""
Download and prepare data from the Malaria Atlas Project

https://data.malariaatlas.org

Data is no longer directly available from a file server but is instead provided via a GeoServer instance.
This can still be retrieved using a standard request call but to determine the proper URL requires initiating the download via their website mapping platform and tracing the corresponding network call. This process only needs to be done once for new datasets; URLs for the datasets listed below have already been identifed.


Current download options:
- pf_incidence_rate: incidence data for Plasmodium falciparum species of malaria (rate per 1000 people)


Unless otherwise specified, all datasets are:
- global
- from 2000-2020
- downloaded as a single zip file containing each datasets (all data in zip root directory)
- single band LZW-compressed GeoTIFF files at 2.5 arcminute resolution
"""

import os
import sys
import shutil
import requests
from copy import copy
from pathlib import Path
from zipfile import ZipFile
from configparser import ConfigParser

import rasterio
from rasterio import windows

#sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
#sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
#sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')

from malaria import MalariaAtlasProject



def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "dataset": config["main"]["dataset"],
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs"
    }

if __name__ == "__main__":

    from dataset import Dataset

    sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
    #sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
    #sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')

    config_dict = get_config_dict()

    class_instance = MalariaAtlasProject(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["dataset"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=config_dict["log_dir"])

