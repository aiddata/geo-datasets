# geo-datasets image

This is an OCI image meant to be built with podman.
It uses images from prefect, and adds the dependencies required to run most dataset ingest scripts.

## Building the image

Follow these instructions if you'd like to develop the image itself, have a copy of it, and/or upload it to a registry.
If you just want to get going, use [jacobwhall/geodata-container](https://hub.docker.com/repository/docker/jacobwhall/geodata-container) as your image.

```shell
cd container
podman build --tag geodata-container .
```


## Pushing the image to Docker Hub

1. Log in to docker in podman
   ```shell
   podman login docker.io
   ```
   It will prompt you for your username and password

2. Push image to docker
   ```shell
   podman push geodata-container docker.io/your-username/geodata-container:latest
   ```
