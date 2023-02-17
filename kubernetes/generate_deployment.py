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
    "image_name": "geodata-container",
    "namespace": "geodata",
    "work_queue": "geodata",
}

template = Template(
    (
        prefect.__module_path__ / "cli" / "templates" / "kubernetes-agent.yaml"
    ).read_text()
)

manifest = template.substitute(manifest_vars)

volume_mounts_injection = [{"mountPath": "/sciclone", "name": "sciclone"}]

volume_injection = [{"name": "sciclone", "persistentVolumeClaim": {"claimName": "pvc0001"}}]

def gen_docs():
    for doc in yaml.load_all(manifest, Loader=yaml.FullLoader):
        if doc["kind"] == "Deployment":
            doc["spec"]["template"]["spec"]["containers"][0]["volumeMounts"] = volume_mounts_injection
            doc["spec"]["template"]["spec"]["volumes"] = volume_injection
            print("this is the deployment")
        yield doc


with open("orion.yaml", "w") as dst:
    yaml.dump_all(gen_docs(), dst)
