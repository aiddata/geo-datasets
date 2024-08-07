# Local development instructions

## Getting Started

1. [Install kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
2. [Install helm](https://helm.sh/docs/intro/install/)
3. [Install podman](https://podman.io/getting-started/installation)
4. [Install kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)

5. Adjust location of local data directory in `kind-config.yaml`
   - Change `./data` to your preferred location **for all worker nodes** (see comments)
   - Make the directory if needed, e.g. `mkdir data`

6. Start your kind cluster
   ```shell
   # delete any existing cluster you may have
   kind delete cluster
   # cd to the correct directory if you used a relative path in step 5
   cd dev
   # the env variable tells kind to use podman instead of docker
   KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --config kind-config.yaml
   ```
   - If you run into issues, there may be additional steps to run kind rootless. [See this documentation page](https://kind.sigs.k8s.io/docs/user/rootless/)

7. Set "geodata" as our default namespace using `kubectl`.
   To work with the newly-created resources in Kubernetes, set your kubectl context use the same namespace.
   If you decided to use a different namespace when running `helm install` below, adjust this command accordingly.
   ```shell
   kubectl config set-context --current --namespace=geodata
   ```
8. Set helm `values.yaml`
   - In the root of repo, copy `values_template.yaml` to `values.yaml`
   - Adjust the values in `values.yaml` to meet your needs.
      - In particular, make sure to set the correct URL and key to access the Prefect API. (You can get these by running `prefect config view` within your Prefect environment)
      - For a local cluster, enable dev mode and set an appropriate data path

9. Build dependencies (dask) for helm cluster
   ```
   helm repo add dask https://helm.dask.org/
   helm dependency build ./helm_chart
   ```

10. Install the helm chart into the local cluster.
   If there is already a namespace called "geodata", remove `--create-namespace`
   ```shell
   # cd to the root of this repository
   cd ..
   helm install --create-namespace --namespace geodata geodata-release ./helm_chart -f values.yaml
   ```
   See "Installing the helm chart" above for more info about what this command does.

You now have everything up and running in a local kind cluster.
See the "Peeking inside the cluster" section below for more on how to use kubectl to manipulate cluster resources.


## Peeking inside the cluster

This is a brief tutorial for viewing the resources inside Kubernetes using `kubectl` and observing their behavior.

1. If you haven't yet, set the default namespace for `kubectl`.
   ```shell
   kubectl config set-context --current --namespace=geodata
   ```

2. The primary resource of interest that always runs is called "prefect-agent".
   It is a deployment that always keeps a container alive running Prefect agent.
   ```shell
   kubectl get deployments
   ```
   You should now see "prefect-agent" in this list of deployments.

3. You can get more info about a Kubernetes resource using the `kubectl describe [RESOURCE TYPE] [RESOURCE NAME]` command.
   See the details of "prefect-agent" by running the following command:
   ```shell
   kubectl describe deployment prefect-agent
   ```

4. In the command output from the last step, read the bottom few lines.
   You should see info about a replica set that Kubernetes has created for this deployment.
   A replica set can hold many identical pods, so that there is always more than one available at any given time.
   However, in our situation the replica set only has one "replica" (pod) in it.
   You can see the list of pods in the replica set by describing the replica set:
   ```shell
   # adjust the name of the replica set to match yours
   kubectl describe replicaset prefect-agent-58f85d6c6
   ```

5. To get a list of pods in this namespace, use the following command:
   ```shell
   kubectl get pods
   ```
   You should see a list of pods that includes one that starts with "prefect-agent-".
   When deployments are run, new pods are created by the prefect-agent pod.

6. To see the logs (output) of a pod, use this command:
   ```shell
   # adjust the name of the pod to match the one you'd like to inspect
   kubectl logs prefect-agent-58f85d6c6-2j5l7
   ```
   If you used a valid Prefect API URL and key to install the helm chart, you should see that the Prefect agent is running.



## ⚠️ Setup local cluster using minikube ⚠️

**warning: these instructions are out-of-date. please use the kind instructions above.**

[minikube](https://minikube.sigs.k8s.io) is cool because it runs a local Kubernetes cluster in VMs.

### Quickstart

1. [Install minikube](https://minikube.sigs.k8s.io/docs/start/)

2. Start minikube, a local Kubernetes cluster to test everything

   The cri-o container runtime is only necessary [if you plan to use podman-env](https://minikube.sigs.k8s.io/docs/handbook/pushing/#comparison-table-for-different-methods), as described in "Custom local images in minikube" below.
   ```shell
   # this is unnecessary unless you are developing local images
   minikube config set container-runtime crio
   minikube start
   ```


### Custom local images in minikube

If you'd rather not use an external image registry (for rapid development, for example), here are instructions for building images directly from podman into minikube.

1. Set up podman to access minikube
   ```shell
   eval $(minikube podman-env)
   ```
   Note that this just `export`s some environment variables, so you'll have to re-run this each time you open a new terminal session

2. Build container into minikube, this makes it available to the deployment we're about to make
   ```shell
   podman --remote build -t geodata-container .
   ```

3. Check that the image made it into minikube
   ```shell
   minikube image ls
   ```
   In the list, you'll hopefully see `localhost/geodata-container:latest`
   You can use that as the name of your image in the helm `values.yaml`.

### Useful minikube commands

- Pausing minikube
  ```shell
  minikube pause
  ```

- Resuming minikube
  ```shell
  minikube resume
  ```

- Resetting minkube
  ```shell
  minikube delete
  ```
  Doing this really deletes everything, so you'll have to start from scratch again.


