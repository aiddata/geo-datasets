# geodata helm chart

## Installation

If you're installing this helm chart on a local cluster, see `dev.md` in the parent directory for instructions.
This section is for installing the helm chart for production needs, with an image already available from a registry somewhere.

1. Create a custom `values.yaml` with your Prefect API info.

   The `values.yaml` file in this directory is meant to stay as-is.
   To override values for your own installation, please create a copy of `values.yaml` with your own settings.
   A template, `/values_template.yaml`, is provided in the root directory of this repository.
   For more information about value overrides, please see [the helm docs](https://helm.sh/docs/chart_template_guide/values_files/).

   The image set in `values.yaml` is only for the Prefect Agent.
   If you'd like to set which image runs the Prefect run, you must edit the appropriate Prefect infrastructure block.
   Instructions for doing so are in `../utilities/README.md`

   If you want to build your own image, follow the instructions in "Creating the image" section below to build the image yourself.

2. Here is an example command for installing the helm chart from the parent directory.
   Please review it carefully before running!
   ```shell
   cd geodata-container
   helm install --create-namespace --namespace geodata --values my-values.yaml geodata-release ./helm_chart
   ```
   - `--namespace geodata` sets which namespace to install the chart in
   - `--create-namespace` tells helm to create the namespace if it doesn't already exist. If the namespace already exists, helm will likely throw an error.
   - `--values my-values.yaml` loads in a custom values file to override defaults in the helm chart.
   - `geodata-release` is the name of the release (installed instance of this chart). You will use this name to upgrade or uninstall this release in the future.
   - `./helm_chart` points helm to the directory where the chart lives


## Upgrading

If you have a previous release (helm lingo for an installation), you can "upgrade" it in-place with an updated chart.
```shell
# list helm releases to get the name of the one you want to upgrade
helm list
# change "geodata-release" to be the correct release name
helm upgrade geodata-release ./helm_chart
```
If the output includes the phrase, "Happy Helming!" you're in the clear :smile:


## Building

This isn't necessary unless you want to update the agent deployment manifest using Prefect's template.
Otherwise, don't worry about it. It's all ready to go.
```shell
pip install prefect
cd helm_chart
make
```

It's a good idea to lint your chart before use.
```shell
# adjust path depending on your current directory
helm lint .
```


## Implementation notes

- Storage persistence is achieved in Kubernetes through the use of [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).
  These represent physical media, whether on a node's filesystem or on a cloud storage service.
  In practice, a persistent volume will be set up for us on the Nova cluster.
  To run this locally, however, you'll need to make one yourself.
  Do this by applying `pv.yaml` to your cluster:
  ```shell
  kubectl apply -f pv.yaml
  ```

- Persistent Volumes are allocated in Kubernetes using Persistent Volume Claims.
  These Claims allow you to divide up large Persistent Volumes to be used by different resources on the cluster.
  This will also likely be managed for us on Nova.
  I've written `pvc.yaml` for local development:
  ```shell
  kubectl apply -f pvc.yaml
  ```

- Pods running within Kubernetes can be assigned Service Accounts, which can then be given role-based access to other resources in the cluster.
  Prefect agent pods need a variety of permissions in order to launch and monitor jobs for each deployment run.
  `serviceaccount.yaml` includes definitions for a Service Account, and a few Roles and Role Bindings for it.
  ```shell
  kubectl apply -f serviceaccount.yaml
  ```
