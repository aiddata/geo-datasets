"""
This script is intended to build and deploy a Prefect flow using settings/parameters defined in an
accompany config.ini for the dataset.

---------------------------------------

Roughly equivalent actions via cli
#
# *** the below commands are just a general starting point, and not meant to run as is. note that there
#     are no parameters specified or storage block creation

# to deploy:
prefect deployment build flow.py:flow_function_name -n "deployment_name" -sb github/existing_storage_block_name -q work_queue_name --apply

# to not immediately deploy remove `--apply` from the above line, then use the build yaml to run the following:
# prefect deployment apply build-deployment.yaml

# to run the deployment
prefect deployment run flow_function_name/deployment_name

# start workqueue
prefect agent start -q 'work_queue_name'

"""

import inspect
import os
import sys
import tomllib
from pathlib import Path

from prefect.runner.storage import GitRepository

from data_manager import Dataset

if len(sys.argv) != 2:
    raise Exception(
        "deploy.py requires input defining which dataset directory to obtain the config.ini from"
    )

dataset_name = sys.argv[1].strip("/")
dataset_dir = Path(os.path.realpath(__file__)).parent.parent / "datasets" / dataset_name

if not dataset_dir.exists():
    raise Exception("dataset directory provided not found in current directory")


sys.path.insert(
    1,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))), dataset_dir
    ),
)

from main import get_config

config_file = dataset_dir / "config.toml"
with open(config_file, "rb") as src:
    config = tomllib.load(src)

# load flow
module_name = config["deploy"]["flow_file_name"]
flow_name = config["deploy"]["flow_name"]
flow_image = "docker.io/jacobwhall/geodata-container:{}".format(
    config["deploy"]["image_tag"]
)
data_manager_version = config["deploy"]["data_manager_version"]


# create and load storage block
git_repo_url = config["repo"]["url"]
git_branch = config["repo"]["branch"]  # branch or tag
git_directory = config["repo"]["directory"]

# -------------------------------------


def flow_import(module_name, flow_name):
    module = __import__(module_name)
    import_flow = getattr(module, flow_name)

    # find the Dataset class
    dataset_name = ""
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj):
            if Dataset in obj.__bases__:
                if dataset_name != "":
                    raise RuntimeError("Multiple Dataset classes found in module!")
                else:
                    dataset_name = obj.name
    if dataset_name == "":
        raise RuntimeError(f"No Dataset class found in module {module_name}")

    return import_flow, dataset_name


# Driver Code
flow, dataset_name = flow_import(module_name, flow_name)


flow.from_source(
    source=GitRepository(
        url=git_repo_url,
        branch=git_branch,
    ),
    entrypoint="{}/{}.py:{}".format(git_directory, module_name, flow_name),
).deploy(
    name=dataset_name,
    work_pool_name=config["deploy"]["work_pool"],
    image=flow_image,
    job_variables={"env": {"DATA_MANAGER_VERSION": data_manager_version}},
    parameters={"config": config},
    version=config["deploy"]["version"],
    build=False,
)
