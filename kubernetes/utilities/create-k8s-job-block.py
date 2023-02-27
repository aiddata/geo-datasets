from prefect.infrastructure import KubernetesJob

json_patches = [
    {
        "op": "add",
        "path": "/spec/template/spec/volumes",
        "value": [
            {"name": "sciclone", "persistentVolumeClaim": {"claimName": "pvc0001"}}
        ],
    },
    {
        "op": "add",
        "path": "/spec/template/spec/containers/0/volumeMounts",
        "value": [{"mountPath": "/sciclone", "name": "sciclone"}],
    },
]

k8s_job = KubernetesJob(
    name="dataset-run",
    namespace="geodata",
    image="localhost/geodata-container",
    customizations=json_patches,
)
k8s_job.save("kubejob", overwrite=True)
