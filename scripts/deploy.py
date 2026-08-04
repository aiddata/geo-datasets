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

import asyncio
import inspect
import json
import os
import sys
import tomllib
from pathlib import Path

from prefect import get_client
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

config_file = dataset_dir / "config.toml"
with open(config_file, "rb") as src:
    config = tomllib.load(src)

# check for .env file and load it if present
if (dataset_dir / ".env").exists():
    with open(dataset_dir / ".env", "r") as f:
        for line in f:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                config[key] = value

# check for box_config.json and, if present, minify and pass it as the
# "box_config" parameter (some datasets, e.g. pm25, expect a Box JWT
# app-auth JSON string rather than a nested config.toml table)
if (dataset_dir / "box_config.json").exists():
    with open(dataset_dir / "box_config.json", "r") as f:
        config["box_config"] = json.dumps(json.load(f))

# load flow
module_name = config["deploy"]["flow_file_name"]
flow_name = config["deploy"]["flow_name"]
flow_image = "ghcr.io/aiddata/geo-datasets:{}".format(config["deploy"]["image_tag"])

# for the "kubernetes" task runner, dask scheduler/worker pods should use the
# same image as the flow-run pod itself, so they have a matching data_manager
# version and Python environment
config.setdefault("run", {})["worker_image"] = flow_image


async def get_work_pool_pvc_claim(pool_name: str) -> str:
    """
    Read the PersistentVolumeClaim name the given work pool's flow-run pods
    actually mount, straight from its live base_job_template - this is the
    same claim dask scheduler/worker pods need to mount so raw_dir/output_dir
    paths resolve identically, and it's the authoritative source for which
    claim a given work pool (e.g. staging vs prod) is really configured with,
    rather than assuming a hardcoded name.
    """
    async with get_client() as client:
        work_pool = await client.read_work_pool(pool_name)
    pod_spec = work_pool.base_job_template["job_configuration"]["job_manifest"][
        "spec"
    ]["template"]["spec"]
    return pod_spec["volumes"][0]["persistentVolumeClaim"]["claimName"]


config["run"]["worker_pvc_claim"] = asyncio.run(
    get_work_pool_pvc_claim(config["deploy"]["work_pool"])
)


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
    parameters={"config": config},
    version=str(config["deploy"]["version"]),
    # The image is built and pushed by CI (.github/workflows/build-image.yml),
    # so deploying must neither rebuild it nor try to push it.
    build=False,
    push=False,
)
