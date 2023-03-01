import sys
from string import Template

import yaml
import prefect

manifest_vars = {
    "namespace": "{{.Values.namespace}}",
    "image_name": "{{.Values.agent_container}}",
    "api_url": "{{.Values.prefect.apiURL}}",
    "api_key": "{{.Values.prefect.apiKey}}",
    "work_queue": "{{.Values.prefect.workQueue}}",
}

manifest = (
        prefect.__module_path__ / "cli" / "templates" / "kubernetes-agent.yaml"
    ).read_text()

# generator that injects our custom config into YAMLs
def gen_docs():
    # for each YAML document in Prefect's template
    for doc in yaml.load_all(manifest, Loader=yaml.FullLoader):
        # if it's the deployment document, we're going to inject some stuff
        if doc["kind"] == "Deployment":
            # inject serviceaccount
            doc["spec"]["template"]["spec"]["serviceAccountName"] = "geodata-launcher"
        yield doc

# retrieve deployment manifest template from Prefect
template = Template(
    yaml.dump_all(gen_docs())
)

# fill out template from Prefect with our values
print(template.substitute(manifest_vars))
