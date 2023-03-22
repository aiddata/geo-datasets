from prefect.infrastructure import KubernetesJob, KubernetesImagePullPolicy

# config
namespace = "geodata"
run_image = "jacobwhall/geodata-container"

# should we have requests and limits?
requests_and_limits: bool = False

# CPU request and limit
cpu_request: int = 3
cpu_limit: int = 5

# memory request and limit (in GiB)
memory_request: int = 1
memory_limit: int = 3

# whether or not to use volume
use_volume = True



# patches

json_patches = []

# add CPU and RAM request and limit amounts
if requests_and_limits:
    json_patches.extend(
        [
            {
                "op": "add",
                "path": "/spec/template/spec/containers/0/resources",
                "value": {
                    "limits": {
                        "cpu": str(cpu_limit),
                        "memory": str(memory_limit) + "Gi",
                    },
                    "requests": {
                        "cpu": str(cpu_request),
                        "memory": str(memory_request) + "Gi",
                    },
                },
            }
        ]
    )

if use_volume:
    # add volume
    json_patches.extend(
        [
            {
                "op": "add",
                "path": "/spec/template/spec/volumes",
                "value": [
                    {"name": "sciclone", "persistentVolumeClaim": {"claimName": "nova-geodata-prod"}}
                ],
            },
            {
                "op": "add",
                "path": "/spec/template/spec/containers/0/volumeMounts",
                "value": [{"mountPath": "/sciclone", "name": "sciclone"}],
            },
        ]
    )

# add service account to job pods
json_patches.append({
    "op": "add",
    "path": "/spec/template/spec/serviceAccountName",
    "value": "dask-job",
})

k8s_job = KubernetesJob(
    name="dataset-run",
    namespace=namespace,
    image=run_image,
    customizations=json_patches,
    image_pull_policy=KubernetesImagePullPolicy.ALWAYS,
)
k8s_job.save("geodata-k8s", overwrite=True)
