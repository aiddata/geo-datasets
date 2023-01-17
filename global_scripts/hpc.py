import math
from pathlib import Path
from warnings import warn
from typing import Optional
from contextlib import AsyncExitStack

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
    def __init__(self, num_procs: int, log_dir=None, **kwargs):
        if log_dir is not None:
            self.log_dir = Path(log_dir)
        else:
            self.log_dir = None

        adapt_max = num_procs
        if "cluster_kwargs" in kwargs and "processes" in kwargs["cluster_kwargs"]:
            adapt_min = num_procs#kwargs["cluster_kwargs"]["processes"]
        else:
            adapt_min = num_procs
        dask_task_runner_kwargs = {
            "cluster_class": PBSCluster,
            "cluster_kwargs": get_cluster_kwargs(**kwargs),
            "adapt_kwargs": {"minimum": adapt_min, "maximum": adapt_max},
        }
        super().__init__(**dask_task_runner_kwargs)

    async def _start(self, exit_stack: AsyncExitStack):
        await super()._start(exit_stack)
        try:
            if self.log_dir is not None:
                with open(self.log_dir / "hpc_job_script", "x") as job_script_log:
                    job_script_log.write(self._cluster.job_script())
        except FileExistsError:
            warn("hpc_job_script already exists in log directory! Skipping without overwriting.")
        except Exception as e:
            warn(f"Error occured while logging HPC job script: {repr(e)}")
