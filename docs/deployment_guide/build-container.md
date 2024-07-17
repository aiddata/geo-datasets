# Building the job-runner Container

!!! warning

    Please make sure your workspace is clean (all changes stashed or committed) to prevent building containers with uncommitted code.

## Build the Container

!!! info
    This image now builds with an unusually high UID, to match that of jwhall's on the HPC systems. To build this container, you will likely need to increase the range value of the available subuids on your system. The easiest way to do this is to append a "0" to the end of your user's line in `/etc/subuid`

1. `cd` to the `kubernetes/containers` directory of the geo-datasets repository
   ```
   cd geo-datasets/kubernetes/containers
   ```
2. Build the container out of the `job-runner` directory
   ```
   podman build --tag geodata-container job-runner/
   ```

## Push the Container

!!! warning

    Only Jacob can push to `jacobwhall/geodata-container` on Docker Hub, which should be fixed.

1. Login to Docker Hub with podman
    1. Run `podman login`
    2. Enter your username and password when prompted
2. Determine the short hash for this commit
3. Push the container to Docker using the short hash
   ```
   podman push geodata-container docker.io/jacobwhall/geodata-container:XXXXXX
   ```
