# geo-datasets images

These are OCI images meant to be built with podman.

The commands below are for developing the images and uploading them to a registry.
If you just want to get going, use the default images in the helm cluster.

## Common elements

Both job-runner and dask-worker have:
- requirements.txt
  - this is a list of requirements meant to be used with pip
  - requirements should match between the images, to the extent possible
- prepare.sh
  - script that downloads the geo_datasets package, and installs it

## job-runner

When Prefect Agent submits a job, that job spins up this image to execute the flow run.
It builds upon a Prefect docker image, adding the dependencies required to run most dataset ingest scripts, and a short initialization script that installs latets version of the geo_datasets package

### building

```shell
cd container
podman build --tag geodata-container .
```

### push to Docker Hub

1. Log in to docker in podman
   ```shell
   podman login docker.io
   ```
   It will prompt you for your username and password

2. Push image to docker
   ```shell
   podman push geodata-container docker.io/your-username/geodata-container:latest
   ```

## dask-worker

When a dask cluster spins up, this image is used for the workers.
It builds upon an image from Dask, adding the dependencies required to run most dataset ingest scripts, and a short initialization script that installs latets version of the geo_datasets package

### building

```shell
cd container
podman build --tag geodata-dask .
```

### push to Docker Hub

1. Log in to docker in podman
   ```shell
   podman login docker.io
   ```
   It will prompt you for your username and password

2. Push image to docker
   ```shell
   podman push geodata-container docker.io/your-username/geodata-container:latest
   ```
