# geodata helm chart

## Installation

If you're installing this on minikube for local development, skip down to "Local development instructions" instead.
This section is for installing the helm chart for production needs, with an image already available from a registry somewhere.

1. Edit `helm_chart/values.yaml` with your Prefect API info.

   If you want to use your own image, follow the instructions in "Creating the image" section below to build the image yourself.

2. Here is an example command for installing the helm chart with 
   ```shell
   cd geodata-container
   helm install --create-namespace --namespace geodata geodata-release ./helm_chart
   ```
   - `--create-namespace` tells helm to create the namespace if it doesn't already exist
   - `--namespace geodata` sets the name of the namespace to use
   - `geodata-release` is the name of the release (basically, installed instance of this chart)
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
