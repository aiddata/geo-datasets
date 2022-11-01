from typing import Optional
from functools import wraps
from abc import ABC, abstractmethod
from prefect import flow
from dask_jobqueue import PBSCluster
from prefect_dask import DaskTaskRunner
from prefect.task_runners import SequentialTaskRunner


class Dataset(ABC):

    @abstractmethod
    def download(self):
        raise NotImplementedError("Datasets must implement a download function")

    @abstractmethod
    def process(self):
        raise NotImplementedError("Datasets must implement a process function")

    def run(self):
        self.download()
        self.process()

    def set_task_runner(
        self,
        cluster_kwargs: Optional[dict]=None,
        adapt_kwargs: Optional[dict]=None,
        run_parallel: bool = True,
    ):

        if run_parallel:

            if cluster_kwargs is None or adapt_kwargs is None:
                prefect_task_runner = DaskTaskRunner
            else:
                dask_task_runner_kwargs = {
                    "cluster_class": PBSCluster,
                    "cluster_kwargs": cluster_kwargs,
                    "adapt_kwargs": adapt_kwargs,
                }

                prefect_task_runner = DaskTaskRunner(**dask_task_runner_kwargs)
        else:
            self.task_runner = SequentialTaskRunner