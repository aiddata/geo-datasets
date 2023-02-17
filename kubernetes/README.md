# A prototype container for geo-datasets

In this document I use `podman`, but `docker` should work similarly

## Building Container

Do this if you'd like to develop the image itself, have a copy of it, and/or upload it to a registry.
Instructions for building into minikube are described below.
```
cd geodata-container
podman build --tag geodata-container .
```

## Set up dev environment (minikube)

1. Start minikube

   The cri-o container runtime is only necessary [if you like using podman-env](https://minikube.sigs.k8s.io/docs/handbook/pushing/#comparison-table-for-different-methods) (as we do in steps 4 and 5 below)
   ```shell
   minikube start --container-runtime=cri-o
   ```

2. Create the namespace "geodata" if it doesn't already exist
   ```shell
   kubectl create namespace geodata
   ```

3. Set "geodata" as our default namespace in kubectl
   ```shell
   kubectl config set-context --current --namespace=geodata
   ```
   
4. Set up podman to access minikube
   ```shell
   eval $(minikube podman-env)
   ```

5. Build container into minikube, this makes it available to the deployment we're about to make
   ```shell
   podman --remote build -t geodata-container .
   ```

6. Check that the image made it into minikube
   ```shell
   minikube image ls
   ```
   In the list, you'll hopefully see `localhost/geodata-container:latest`

## Deploy container to Kubernetes

1. Storage persistence is achieved in Kubernetes through the use of [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).
   These represent physical media, whether on a node's filesystem or on a cloud storage service.
   In practice, a persistent volume will be set up for us on the Nova cluster.
   To run this locally, however, you'll need to make one yourself.
   Do this by applying `pv.yaml` to your cluster:
   ```shell
   kubectl apply -f pv.yaml
   ```

2. Persistent Volumes are allocated in Kubernetes using Persistent Volume Claims.
   These Claims allow you to divide up large Persistent Volumes to be used by different resources on the cluster.
   This will also likely be managed for us on Nova.
   I've written `pvc.yaml` for local development:
   ```shell
   kubectl apply -f pvc.yaml
   ```

3. Now it's time to deploy our container
   ```shell
   conda activate geodata38
   # review generate_manifest.py and change any necessary variables
   python generate_manifest.py > orion.yaml
   kubectl apply -f orion.yaml
   ```

   The deployment should now be spinning up in minikube!
   Check on it by running:
   ```shell
   kubectl describe deployment prefect-agent
   ```

## Using minikube

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
