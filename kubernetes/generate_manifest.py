import sys
from string import Template

import yaml

import prefect
from prefect.settings import (
    PREFECT_API_KEY,
    PREFECT_API_URL,
)

manifest_vars = {
    "api_url": PREFECT_API_URL.value(),
    "api_key": PREFECT_API_KEY.value(),
    "image_name": "localhost/geodata-container",
    "namespace": "geodata",
    "work_queue": "geodata",
}

# retrieve deployment manifest template from Prefect
template = Template(
    (
        prefect.__module_path__ / "cli" / "templates" / "kubernetes-agent.yaml"
    ).read_text()
)

# generator that injects our custom config into YAMLs
def gen_docs():
    # for each YAML document in Prefect's template
    for doc in yaml.load_all(manifest, Loader=yaml.FullLoader):
        # if it's the deployment document, we're going to inject some stuff
        if doc["kind"] == "Deployment":
            # inject serviceaccount
            doc["spec"]["template"]["spec"]["serviceAccountName"] = "geodata-launcher"
        yield doc

# write generated YAML to stdout
yaml.dump_all(gen_docs(), sys.stdout)
