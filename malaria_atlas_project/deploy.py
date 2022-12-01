
from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from main import get_config_dict

# load flow
module_name = "flow"
flow_name = "malaria_atlas_project"

def flow_import(module_name, flow_name):
    module = __import__(module_name)
    import_flow = getattr(module, flow_name)
    return import_flow

# Driver Code
flow = flow_import(module_name, flow_name)



# create and load storage block

block_name = "geo-datasets-github"
block_repo = "https://github.com/aiddata/geo-datasets"
block_reference = 'develop' # branch or tag
block_repo_dir = "malaria_atlas_project"

block = GitHub(
    repository=block_repo,
    reference=block_reference,
    #access_token=<my_access_token> # only required for private repos
)
# block.get_directory(block_repo_dir)
block.save(block_name, overwrite=True)


# # load a pre-defined block and specify a subfolder of repo
storage = GitHub.load(block_name)#.get_directory(block_repo_dir)
storage.save("malaria_atlas_project")

# build deployment
deployment = Deployment.build_from_flow(
    flow=flow,
    name="malaria_atlas_project_pf_prevalence_rate",
    version=6,
    work_queue_name="geo-datasets",
    storage=storage,
    parameters=get_config_dict()
)

# apply deployment
deployment.apply()



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