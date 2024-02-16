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

import sys
import os
from configparser import ConfigParser

from prefect.deployments import Deployment
from prefect.runner.storage import GitRepository


if len(sys.argv) != 2:
    raise Exception(
        "deploy.py requires input defining which dataset directory to obtain the config.ini from"
    )

dataset_dir = sys.argv[1].strip("/")

if dataset_dir not in os.listdir(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
):
    raise Exception("dataset directory provided not found in current directory")


sys.path.insert(
    1,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))), dataset_dir
    ),
)

from main import get_config_dict


config_file = dataset_dir + "/config.ini"
config = ConfigParser()
config.read(config_file)


# load flow
module_name = config["deploy"]["flow_file_name"]
flow_name = config["deploy"]["flow_name"]
flow_image = "docker.io/jacobwhall/geodata-container:77fb4ec"
data_manager_version = config["deploy"]["data_manager_version"]


# create and load storage block
git_repo = config["github"]["repo"]
git_branch = config["github"]["branch"]  # branch or tag
git_directory = config["github"]["directory"]

# -------------------------------------


def flow_import(module_name, flow_name):
    module = __import__(module_name)
    import_flow = getattr(module, flow_name)
    return import_flow


# Driver Code
flow = flow_import(module_name, flow_name)

flow.from_source(
    source=GitRepository(
        url=git_repo,
        branch=git_branch,
    ),
    entrypoint="{}/{}.py:{}".format(git_directory, module_name, git_directory),
).deploy(
    name=config["deploy"]["deployment_name"],
    work_pool_name=config["deploy"]["work_pool"],
    image=flow_image,
    job_variables={"env": {"DATA_MANAGER_VERSION": data_manager_version} },
    parameters=get_config_dict(config_file),
    version=config["deploy"]["version"],
    build=False,
)
