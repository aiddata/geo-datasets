# A prototype container for geo-datasets

In this document I use `podman`, but `docker` should work similarly

## Building Image

Do this if you'd like to develop the image itself, have a copy of it, and/or upload it to a registry.
Instructions for building into minikube are described below in "Local development instructions".
```
cd container
podman build --tag geodata-container .
```

## Installing the helm chart

If you're installing this on minikube for local development, skip down to "Local development instructions" instead.
This section is for installing the helm chart for production needs, with an image already available from a registry somewhere.

1. Edit `helm_chart/values.yaml` with your Prefect API info, and the location of the image.

2. Here is an example command for installing the helm chart with 
   ```shell
   cd geodata-container
   helm install --create-namespace --namespace geodata geodata-release ./helm_chart
   ```
   - `--create-namespace` tells helm to create the namespace if it doesn't already exist
   - `--namespace geodata` sets the name of the namespace to use
   - `geodata-release` is the name of the release (basically, installed instance of this chart)
   - `./helm_chart` points helm to the directory where the chart lives

## Building helm chart

This isn't necessary unless you want to update the agent deployment manifest using Prefect's template.
Otherwise, don't worry about it. It's all ready to go.
```shell
pip install prefect
cd helm_chart
make
```

## Local development instructions (using minikube)

1. Install minikube and install helm if you haven't already.

2. Start minikube, a local Kubernetes cluster to test everything

   The cri-o container runtime is only necessary [if you like using podman-env](https://minikube.sigs.k8s.io/docs/handbook/pushing/#comparison-table-for-different-methods) (as we do in steps 4 and 5 below)
   ```shell
   minikube config set container-runtime crio
   minikube start
   ```

3. Set up podman to access minikube
   ```shell
   eval $(minikube podman-env)
   ```
   Note that this just `export`s some environment variables, so you'll have to re-run this each time you open a new terminal session

4. Build container into minikube, this makes it available to the deployment we're about to make
   ```shell
   podman --remote build -t geodata-container .
   ```

5. Check that the image made it into minikube
   ```shell
   minikube image ls
   ```
   In the list, you'll hopefully see `localhost/geodata-container:latest`

6. Adjust the values in `helm_chart/values.yaml` to meet your needs.
   In particular, make sure to set the correct URL and key to access the Prefect API.

7. Install the helm chart into the minikube cluster
   ```shell
   helm install --create-namespace --namespace geodata geodata-release ./helm_chart
   ```
   See "Installing the helm chart" above for more info about what this command does.

That's it! You now have everything up and running in your minikube cluster.


## Peeking inside the cluster

This is a brief tutorial for viewing the resouces inside Kubernetes using `kubectl` and observing their behavior.

1. Set "geodata" as our default namespace using `kubectl`.
   If you decided to use a different namespace when running `helm install` above, adjust this command accordingly.
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

4. In the description from step 3, read the bottom few lines.
   You should see info about a replica set that Kubernetes has created for this deployment.
   A replica set can hold many identical pods so that there is always one available, even if one dies.
   However, in our situation the replica set only has one replica (pod) in it.
   You can see the list of pods in the replica set by describing it:
   ```shell
   # adjust the name of the replica set to match yours
   kubectl describe replicaset prefect-agent-58f85d6c6
   ```

5. To get a list of pods in this namespace, use the following command:
   ```shell
   kubectl get pods
   ```
   You should see a list of pods that includes one that starts with "prefect-agent-".
   When deployments are run, new pods will appear to run each job, which were created by the prefect-agent pod.

6. To see the logs (output) of a pod, use this command:
   ```shell
   # adjust the name of the pod to match the one you'd like to inspect
   kubectl logs prefect-agent-58f85d6c6-2j5l7
   ```
   If you used a valid Prefect API URL and key to install the helm chart, you should see that the Prefect agent is running.


## Create kubernetes-job infrastructure block in Prefect

I wrote a script that creates a Kubernetes Job infrastructure block in Prefect that you can use to deploy things into the cluster.
Here's how you use it:

```shell
conda activate geodata38
python utilities/create-k8s-job-block.py
```

## Useful minikube commands

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
