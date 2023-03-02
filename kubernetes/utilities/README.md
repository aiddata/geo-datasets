# Utilities

## Create kubernetes-job infrastructure block in Prefect

I wrote a script that creates a Kubernetes Job infrastructure block in Prefect.
This infrastructure block provides the info Prefect Agent needs to submit the deployment run as a Kubernetes job rather than running it as a local process.
Here's how you use it:

```shell
conda activate geodata38
# edit config variables in create-k8s-job-block.py
python create-k8s-job-block.py
```
