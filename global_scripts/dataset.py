import os
import csv
import logging
import multiprocessing
from pathlib import Path
from typing import Optional
from datetime import datetime
from collections import namedtuple
from abc import ABC, abstractmethod
from collections.abc import Sequence


"""
A namedtuple that represents the results of one task
You can access a status code, for example, using TaskResult.status_code or TaskResult[0]
"""
TaskResult = namedtuple("TaskResult", ["status_code", "status_message", "result"])

class ResultTuple(Sequence):
    """
    This is an immutable sequence designed to hold TaskResults
    It also keeps track of the name of a run and the time it started
    ResultTuple.results() returns a list of results from each task
    """
    def __init__(self, iterable, name, timestamp=datetime.today()):
        self.elements = []
        for value in iterable:
            if isinstance(value, TaskResult):
                self.elements.append(value)
            else:
                raise ValueError("ResultTuples must only consist of TaskResult namedtuples!")
        self.name = name
        self.timestamp = timestamp

    def __getitem__(self, index):
        return self.elements[index]

    def __len__(self):
        return len(self.elements)

    def __repr__(self):
        success_count = sum(1 for t in self.elements if t.status_code == 0)
        error_count = len(self.elements) - success_count
        return f"<ResultTuple named \"{self.name}\" with {success_count} successes, {error_count} errors>"

    def results(self):
        results = [t.result for t in self.elements if t.status_code == 0]
        if len(results) < len(self.elements):
            logging.getLogger("dataset").warning(f"results() function for ResultTuple {self.name} skipping errored tasks")
        return results


class Dataset(ABC):
    """
    This is the base class for Datasets, providing functions that manage task runs and logs
    """

    @abstractmethod
    def main(self):
        """
        Dataset child classes must implement a main function
        This is the function that is called when Dataset.run() is invoked
        """
        raise NotImplementedError("Dataset classes must implement a main function")


    def get_logger(self):
        """
        This function will return a logger that implements the Python logging API:
        https://docs.python.org/3/library/logging.html

        If you are using Prefect, the logs will be managed by Prefect
        """
        if self.backend == "prefect":
            from prefect import get_run_logger
            return get_run_logger()
        else:
            return logging.getLogger("dataset")


    def error_wrapper(self, func, args):
        """
        This is the wrapper that is used when running individual tasks
        It will always return a TaskResult!
        """
        try:
            return TaskResult(0, "Success", func(*args))
        except Exception as e:
            return TaskResult(1, repr(e), None)


    def run_serial_tasks(self, name, func, input_list):
        """
        Run tasks in serial (locally), given a function and list of inputs
        This will always return a list of TaskResults!
        """
        return [self.error_wrapper(func, i) for i in input_list]


    def run_concurrent_tasks(self, name, func, input_list):
        """
        Run tasks concurrently (locally), given a function a list of inputs
        This will always return a list of TaskResults!
        """
        with multiprocessing.Pool(10) as pool:
            results = pool.starmap(self.error_wrapper, [(func, i) for i in input_list], chunksize=self.chunksize)
        return results


    def run_prefect_tasks(self, name, func, input_list):
        """
        Run tasks using Prefect, using whichever task runner decided in self.run()
        This will always return a list of TaskResults!
        """

        from prefect import task

        @task(name=name)
        def task_wrapper(self, func, inputs):
            return self.error_wrapper(func, inputs)

        futures =  [task_wrapper.submit(self, func, i) for i in input_list]
        return [f.result() for f in futures]


    def run_mpi_tasks(self, name, func, input_list):
        """
        Run tasks using MPI, requiring the use of `mpirun`
        self.pool is an MPIPoolExecutor initialized by self.run()
        This will always return a list of TaskResults!
        """
        from mpi4py.futures import MPIPoolExecutor
        with MPIPoolExecutor(max_workers=self.mpi_max_workers, chunksize=self.chunksize) as pool:
            futures = []
            for i in input_list:
                futures.append(pool.submit(self.error_wrapper, func, i))
        return [f.result() for f in futures]


    def run_tasks(self,
                  func,
                  input_list,
                  retries: int=0,
                  allow_futures: bool=True,
                  name: Optional[str]=None):
        """
        Run a bunch of tasks, calling one of the above run_tasks functions
        This is the function that should be called most often from self.main()
        It will return a ResultTuple of TaskResults
        """

        timestamp = datetime.today()

        if name is None:
            name = func.__name__

        if self.backend == "serial":
            results = self.run_serial_tasks(name, func, input_list)
        elif self.backend == "concurrent":
            results = self.run_concurrent_tasks(name, func, input_list)
        elif self.backend == "prefect":
            results = self.run_prefect_tasks(name, func, input_list)
        elif self.backend == "mpi":
            results = self.run_mpi_tasks(name, func, input_list)
        else:
            raise ValueError("Requested backend not recognized. Have you called this Dataset's run function?")

        if len(results) == 0:
            raise ValueError(f"Task run {name} yielded no results. Did it receive any inputs?")

        return ResultTuple(results, name, timestamp)


    def log_run(self,
                results,
                expand_results: list=[],
                time_format_str: str="%Y_%m_%d_%H_%M"):
        """
        Log a task run
        Given a ResultTuple (usually from run_tasks), and save its logs to a CSV file
        time_format_str sets the timestamp format to use in the CSV filename

        expand_results is an optional set of labels for each item in TaskResult.result
          - None values in expand_results will exclude that column from output
          - if expand_results is an empty list, each TaskResult's result value will be
            written as-is to a "results" column in the CSV
        """
        time_str = results.timestamp.strftime(time_format_str)
        log_file = self.log_dir / f"{results.name}_{time_str}.csv"

        expansion_spec = []
        should_expand_results = False
        for i, h in enumerate(expand_results):
            if h is not None:
                expansion_spec.append(h, i)
                should_expand_results = True

        fieldnames = ["status_code", "status_message"]
        rows_to_write = []
        if should_expand_results:
            for h, _ in expansion_spec:
                fieldnames.append(h)
            for r in results:
                rows_to_write.append(r[:1].extend([r[i] for _, i in expansion_spec]))
        else:
            fieldnames.append("results")
            rows_to_write = [list(r) for r in results]

        with open(log_file, "w", newline="") as lf:
            writer = csv.writer(lf)
            writer.writerow(fieldnames)
            writer.writerows(rows_to_write)


    def run(
        self,
        backend: Optional[str]=None,
        task_runner: Optional[str]=None,
        run_parallel: bool=False,
        max_workers: Optional[int]=None,
        chunksize: int=1,
        log_dir: str="logs",
        logger_level=logging.INFO,
        **kwargs):
        """
        Run a dataset
        Calls self.main() with a backend e.g. "prefect"
        This is how Datasets should usually be run
        """

        timestamp = datetime.today()

        self.log_dir = Path(log_dir)
        self.chunksize = chunksize
        os.makedirs(self.log_dir, exist_ok=True)

        # Allow datasets to set their own default max_workers
        if max_workers is None and hasattr(self, "max_workers"):
            max_workers = self.max_workers

        # If dataset doesn't come with a name use its class name
        if not self.name:
            self.name = self._type()

        if backend == "prefect":
            self.backend = "prefect"

            from prefect import flow
            from prefect.task_runners import SequentialTaskRunner, ConcurrentTaskRunner

            if task_runner == "sequential":
                tr = SequentialTaskRunner
            elif task_runner == "dask":
                from prefect_dask import DaskTaskRunner
                tr = DaskTaskRunner(**kwargs)
            elif task_runner == "concurrent" or task_runner is None:
                tr = ConcurrentTaskRunner
            else:
                raise ValueError("Prefect task runner not recognized")

            @flow(task_runner=tr, name=self.name)
            def prefect_main_wrapper():
                self.main()

            prefect_main_wrapper()

        else:
            logger = logging.getLogger("dataset")
            logger.setLevel(logger_level)
            logger.addHandler(logging.StreamHandler())

            if backend == "mpi":

                self.backend = "mpi"
                self.mpi_max_workers = max_workers

                self.main()

            elif backend == "local" or backend is None:
                if run_parallel:
                    self.backend = "concurrent"
                else:
                    self.backend = "serial"
                self.main()

            else:
                raise ValueError(f"Backend {backend} not recognized.")
