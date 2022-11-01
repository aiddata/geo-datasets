
from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from utility import load_parameters

# load flow
module_name = "flow"
flow_name = "data_flow"

def flow_import(module_name, flow_name):
    module = __import__(module_name)
    import_flow = getattr(module, flow_name)
    return import_flow

# Driver Code
flow = flow_import(module_name, flow_name)



# create and load storage block

block_name = "geodata-github"
block_repo = "https://github.com/sgoodm/geo-datasets"
block_repo_dir = "malaria_atlas_project"

block = GitHub(
    repository=block_repo,
    #access_token=<my_access_token> # only required for private repos
)
block.get_directory(block_repo_dir) # specify a subfolder of repo
block.save(block_name)

storage = GitHub.load(block_name) # load a pre-defined block




# build deployment
deployment = Deployment.build_from_flow(
    flow=flow,
    name="malaria_atlas_project_pf_prevalence_rate",
    version=1,
    work_queue_name="geo-datasets",
    storage=storage,
    parameters=load_parameters()
)

# apply deployment
deployment.apply()



"""
to run a flow from deployment (from cli):

prefect deployment run malaria_atlas_project_pf_prevalence_rate/data_flow

"""
