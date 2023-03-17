# Tools for running [geo-datasets](https://github.com/aiddata/geo-datasets) in Kubernetes

## Running in Local Cluster

Please see [`dev/README.md`](dev/README.md) for documentation on how to set up a local development environment.


## Helm Chart

There is a helm chart that lives in the [`helm_chart`](helm_chart) directory.
See [`helm_chart/README.md`](helm_chart/README.md) for documentation on developing and installing it.


## Containers

The [`containers`](containers) directory holds container definitions needed to build the custom images used by the helm chart.
See [`containers/README.md`](containers/README.md) for documentation on how to build them and push them to a registry.


## Utilities

In the [`utilities`](utilities) directory lives a script that will initialize a Prefect infrastructure block for Kubernetes.
See [`utilities/README.md`](utilities/README.md) for more information.
