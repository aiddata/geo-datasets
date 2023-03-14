"""
This script deploys a dataset to Prefect, using the settings from its config.ini file.

Basic usage:

`python global_scripts/deploy.py [DATASET]`

You can specify a Kubernetes Job infrastructure block as well.
Run the following command for more detailed information:

`python global_scripts/deploy.py --help`

---------------------------------------

# Roughly equivalent actions via CLI

*** the below commands are just a general starting point, and not meant to run as is. note that there
    are no parameters specified or storage block creation

## to deploy:
`prefect deployment build flow.py:flow_function_name -n "deployment_name" -ib infra_block_name -sb github/existing_storage_block_name -q work_queue_name --apply`

- you can remove "-ib infra_block_name" if you'd like Prefect agent to run the flow locally (rather than on Kubernetes, for example)
- to not immediately deploy remove `--apply` from the above line, then use the build yaml to run the following:
  `prefect deployment apply build-deployment.yaml`

## to run the deployment
`prefect deployment run flow_function_name/deployment_name`

## start agent with correct work queue
`prefect agent start -q 'work_queue_name'`

"""

import os
import sys
from configparser import ConfigParser

import click
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from prefect.infrastructure.kubernetes import KubernetesJob

def flow_import(module_name, flow_name):
    module = __import__(module_name)
    import_flow = getattr(module, flow_name)
    return import_flow


@click.command()
@click.argument("dataset")
@click.option("--kubernetes-job-block", default=None, help="Name of Kubernetes Job block to use")
def deploy(dataset, kubernetes_job_block):
    # find dataset directory
    dataset_dir = dataset.strip("/")
    if dataset_dir not in os.listdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))):
        raise Exception("dataset directory provided not found in current directory")
    else:
        click.echo(f"Found dataset {dataset}")

    # find and import the get_config_dict function for the dataset
    click.echo("Finding get_config_dict function for dataset...")
    sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), dataset_dir))
    from main import get_config_dict

    # find and parse dataset config file
    click.echo("Finding config.ini file for dataset...")
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

    # Driver Code
    flow = flow_import(module_name, flow_name)

    # load a pre-defined block and specify a subfolder of repo
    storage = GitHub.load(block_name)#.get_directory(block_repo_dir)

    infra_overrides = {
        "customizations": [],
    }

    for request_type in ("limit", "request"):
        for resource in ("cpu", "memory"): 
            config_key = f"{resource}_{request_type}"
            if config.has_option("run", config_key):
                click.echo(f"Adding resource {request_type}: {amount} for {resouce}")
                amount = str(config["run"][config_key])
                if resource == "memory":
                    amount += "Gi"
                
                infra_overrides["customizations"].append({
                    "op": "replace",
                    "path": f"/spec/template/spec/containers/0/resources/{request_type}s/{resource}",
                    "value": amount,
                })
                    

    deployment_options = {
        "flow": flow,
        "name": config["deploy"]["deployment_name"],
        "version": config["deploy"]["version"],
        # "work_queue_name": "geo-datasets",
        "work_queue_name": config["deploy"]["work_queue"],
        "storage": storage,
        "path": block_repo_dir,
        "infra_overrides": infra_overrides,
        # "skip_upload": True,
        "parameters": get_config_dict(config_file),
        "apply": True,
    }

    # find Kubernetes Job Block, if one was specified
    if kubernetes_job_block is None:
        click.echo("No Kubernetes Job Block will be used.")
    else:
        click.echo(f"Using Kubernetes Job Block: {kubernetes_job_block}")
        deployment_options["infrastructure"] = KubernetesJob.load(kubernetes_job_block)

    # build deployment
    deployment = Deployment.build_from_flow(**deployment_options)

    click.echo("Done!")


if __name__ == "__main__":
    deploy()
