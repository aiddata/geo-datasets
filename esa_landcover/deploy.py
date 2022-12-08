import sys
sys.path.append('global_scripts')

from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from main import get_config_dict

# load flow
module_name = "flow"
flow_name = "esa_landcover"


# create and load storage block

block_name = "geo-datasets-github-esa"
block_repo = "https://github.com/aiddata/geo-datasets.git"
block_reference = 'esa_landcover' #'develop' # branch or tag
block_repo_dir = "esa_landcover"

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

# load a pre-defined block and specify a subfolder of repo
storage = GitHub.load(block_name)

# build deployment
deployment = Deployment.build_from_flow(
    flow=flow,
    name="esa_landcover",
    version=1,
    # work_queue_name="geo-datasets",
    work_queue_name="geodata",
    storage=storage,
    path="esa_landcover",
    # skip_upload=True,
    parameters=get_config_dict("esa_landcover/config.ini"),
    apply=True
)

# alternative to apply deployment after creating build
# deployment.apply()


# prefect deployment build flow.py:esa_landcover -n "ESA Landcover" -sb github/geo-datasets-github -q geodata --apply

# prefect deployment run esa-landcover/esa_landcover



"""
to run a flow from deployment (from cli):

prefect deployment run esa_landcover/esa_landcover


to activate associated queue (from cli):

prefect agent start --work-queue geodata

"""


"""
run all via cli:

prefect deployment build esa_landcover/flow.py:esa_landcover -n "ESA Landcover" -q geodata

prefect deployment apply esa_landcover-deployment.yaml

prefect deployment run 'esa-landcover/ESA Landcover'

prefect agent start -q geodata

"""
