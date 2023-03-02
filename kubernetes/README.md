# Tools for running [geo-datasets](https://github.com/aiddata/geo-datasets) in Kubernetes

## Getting started

Please see [`dev.md`](dev.md) for documentation on how to set up a local development environment and run these tools.


## Helm Chart

There is a helm chart that lives in the [`helm_chart`](helm_chart) directory.
See [`helm_chart/README.md`](helm_chart/README.md) for instructions on how to develop and use it.


## Container

There is a Containerfile and its dependencies that live in the [`container`](container) directory.
See [`container/README.md`](container/README.md) for instructions on how to build it and push it to a registry.


## Utilities

In the [`utilities`](utilities) directory lives a script that will initialize a Prefect infrastructure block for Kubernetes.
See [`utilities/README.md`](utilities/README.md) for more information.
