import sys
from configparser import ConfigParser

from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from main import get_config_dict

sys.path.append('global_scripts')

config_file = "malaria_atlas_project/config.ini"
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
    version=6,
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


# prefect deployment build flow.py:malaria_atlas_project -n "test_deploy105" -sb github/geo-datasets-github2 -q geodata --apply

# prefect deployment run malaria-atlas-project/malaria_atlas_project_pf_prevalence_rate55



"""
to run a flow from deployment (from cli):

prefect deployment run data-flow/malaria_atlas_project_pf_prevalence_rate


to activate associated queue (from cli):

prefect agent start --work-queue geo-datasets


"""

"""
run all via cli:

prefect deployment build ./flow.py:data_flow -n malaria_atlas_project_pf_prevalence_rate2 -q geo-datasets2

prefect deployment apply data_flow-deployment.yaml

prefect deployment run data-flow/malaria_atlas_project_pf_prevalence_rate3

prefect agent start -q 'geo-datasets3'

"""