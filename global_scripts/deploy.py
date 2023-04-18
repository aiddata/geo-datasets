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
from prefect.filesystems import GitHub



if len(sys.argv) != 2:
    raise Exception("deploy.py requires input defining which dataset directory to obtain the config.ini from")

dataset_dir = sys.argv[1].strip("/")

if dataset_dir not in os.listdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))):
    raise Exception("dataset directory provided not found in current directory")


sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), dataset_dir))

from main import get_config_dict


config_file = dataset_dir + "/config.ini"
config = ConfigParser()
config.read(config_file)


# load flow
module_name = config["deploy"]["flow_file_name"]
flow_name = config["deploy"]["flow_name"]


# create and load storage block

block_name = config["deploy"]["storage_block"]
block_repo = config["github"]["repo"]
block_reference = config["github"]["branch"] # branch or tag
block_repo_dir = config["github"]["directory"]

block = GitHub(
    repository=block_repo,
    reference=block_reference,
    #access_token=<my_access_token> # only required for private repos
)
# block.get_directory(block_repo_dir)
block.save(block_name, overwrite=True)

# -------------------------------------

def flow_import(module_name, flow_name):
    module = __import__(module_name)
    import_flow = getattr(module, flow_name)
    return import_flow

# Driver Code
flow = flow_import(module_name, flow_name)

# # load a pre-defined block and specify a subfolder of repo
storage = GitHub.load(block_name)#.get_directory(block_repo_dir)

# build deployment
deployment = Deployment.build_from_flow(
    flow=flow,
    name=config["deploy"]["deployment_name"],
    version=config["deploy"]["version"],
    # work_queue_name="geo-datasets",
    work_queue_name=config["deploy"]["work_queue"],
    storage=storage,
    path=block_repo_dir,
    # skip_upload=True,
    parameters=get_config_dict(config_file),
    apply=True
)

# alternative to apply deployment after creating build
# deployment.apply()
