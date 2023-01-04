import math
from typing import Optional
from dask_jobqueue import PBSCluster
from prefect_dask.task_runners import DaskTaskRunner

vortex_cluster_kwargs = {
    "shebang": "#!/bin/tcsh",
    "resource_spec": "nodes=1:c18a:ppn=12",
    "cores": 12,
    "processes": 12,
    "memory": "30GB",
    "interface": "ib0",
}

# these have not yet been tuned
hima_cluster_kwargs = {
    "shebang": "#!/bin/tcsh",
    "resource_spec": "nodes=1:hima:ppn=32",
    "cores": 3,
    "processes": 3,
    "memory": "30GB",
    "interface": "ib0",
}


def get_cluster_kwargs(
    job_name: str,
    cluster: str="vortex",
    cores_per_process: Optional[int] = None,
    walltime: str = "01:00:00",
    **kwargs
) -> dict:
    if cluster == "vortex":
        cluster_kwargs = vortex_cluster_kwargs
    elif cluster == "hima":
        cluster_kwargs = hima_cluster_kwargs
    else:
        if 'cluster_kwargs' not in kwargs:
            raise ValueError("Cluster name not recognized")
    cluster_kwargs["name"] = job_name
    cluster_kwargs["walltime"] = walltime
    if 'cluster_kwargs' in kwargs:
        cluster_kwargs.update(kwargs['cluster_kwargs'])
        del kwargs['cluster_kwargs']

    cluster_kwargs.update(kwargs)

    if cores_per_process:
        cluster_kwargs["processes"] = math.floor(
            cluster_kwargs["cores"] / cores_per_process
        )
    return cluster_kwargs


def hpc_dask_cluster(num_procs: int, **kwargs) -> PBSCluster:
    cluster_kwargs = get_cluster_kwargs(**kwargs)
    cluster = PBSCluster(**cluster_kwargs)
    cluster.scale(num_procs)
    return cluster


class HPCDaskTaskRunner(DaskTaskRunner):
    def __init__(self, num_procs: int, **kwargs):
        dask_task_runner_kwargs = {
            "cluster_class": PBSCluster,
            "cluster_kwargs": get_cluster_kwargs(**kwargs),
            "adapt_kwargs": {"minimum": 5, "maximum": num_procs},
        }
        super().__init__(**dask_task_runner_kwargs)
