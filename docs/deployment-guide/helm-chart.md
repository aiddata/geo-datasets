# Deploying the Helm Chart

The helm chart for geo-datasets is relatively straightforward.
Below is a table of helm chart values, and what they control.

## Required Values

These values must be specified when deploying this helm chart.

| Key              | Type   | Description            |
| ---------------- | ------ | ---------------------- |
| `prefect.apiURL` | string | Prefect Server API URL |
| `prefect.apiKey` | string | Prefect Server API key |

## Optional Values

Usually these values should not be overridden when deploying this helm chart.
If a permanent change needs to be made, consider updating the default value directly in the geo-datasets repository.

| Key                  | Type   | Description                 |
| -------------------- | ------ | -------------------------   |
| `workerContainer`    | string | Image to use for workers    |
| `prefect.workPool`   | string | Name of Prefect work pool   |
| `prefect.replicas`   | int    | Number of workers to deploy |
| `prod.nfs.address`   | string | IP address of NFS server    |
| `prod.nfs.mountPath` | string | Path on NFS server to mount |
| `dev.enabled`        | bool   | Delete resource             |
