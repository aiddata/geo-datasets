# Debugging Jobs in Kubernetes

Ideally, our datasets run without hitch and a green checkbox appears in the Prefect UI upon completion.
Unfortunately this isn't always the case, and it's sometimes useful to get a picture of what's going on in the pods themselves as they run in kubernetes.
Below are some tips for doing so.

!!! info

    Replace `geo-datasets-namespace` in the commands below with the namespace you've deployed the geo-datasets helm chart to.

## Print Pod Logs

List the pods currently running (or recently errored/completed):
```
kubectl get pods --namespace geo-datasets-namespace
```

Print the logs from a specific pod:
```
kubectl logs name-of-pod --namespace geo-datasets-namespace
```

