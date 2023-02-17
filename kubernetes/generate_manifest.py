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

# fill out template from Prefect with our values
manifest = template.substitute(manifest_vars)

volume_mounts_injection = [{"mountPath": "/sciclone", "name": "sciclone"}]

volume_injection = [{"name": "sciclone", "persistentVolumeClaim": {"claimName": "pvc0001"}}]

# generator that injects our custom config into YAMLs
def gen_docs():
    # for each YAML document in Prefect's template
    for doc in yaml.load_all(manifest, Loader=yaml.FullLoader):
        # if it's the deployment document, we're going to inject some stuff
        if doc["kind"] == "Deployment":
            # inject volumeMounts
            doc["spec"]["template"]["spec"]["containers"][0]["volumeMounts"] = volume_mounts_injection
            # inject volumes
            doc["spec"]["template"]["spec"]["volumes"] = volume_injection
        yield doc

# write generated YAML to stdout
yaml.dump_all(gen_docs(), sys.stdout)
