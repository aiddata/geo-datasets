from time import sleep
from prefect import task, flow
from prefect_dask import DaskTaskRunner
"""
from dask_kubernetes import HelmCluster

cluster_kwargs = {
    "release_name": "geodata-release",
}

helm_cluster_runner = DaskTaskRunner(cluster_class=HelmCluster, cluster_kwargs=cluster_kwargs)
"""

@task
def greet(name:str="Jacob"):
    sleep(30)
    print(f"Hello, {name}!")


# @flow(task_runner=helm_cluster_runner)
@flow
def greet_everyone():
    names = ["Bob", "Carol", "Steve", "Jacob", "Julia"]
    for n in names:
        greet.submit(n)
        with open(f'/sciclone/{n}.txt', 'w') as f:
            f.write(n)

if __name__ == "__main__":
    greet_everyone()
