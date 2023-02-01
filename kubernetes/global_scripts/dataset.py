import os
import csv
import time
import logging
import multiprocessing
from pathlib import Path
from typing import Optional
from datetime import datetime
from collections import namedtuple
from abc import ABC, abstractmethod
from concurrent.futures import wait
from collections.abc import Sequence


"""
A namedtuple that represents the results of one task
You can access a status code, for example, using TaskResult.status_code or TaskResult[0]
"""
TaskResult = namedtuple("TaskResult", ["status_code", "status_message", "args", "result"])

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

    def args(self):
        args = [t.args for t in self.elements if t.status_code == 0]
        if len(args) < len(self.elements):
            logging.getLogger("dataset").warning(f"args() function for ResultTuple {self.name} skipping errored tasks")
        return args

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
        for try_no in range(self.retries + 1):
            try:
                return TaskResult(0, "Success", args, func(*args))
            except Exception as e:
                if try_no < self.retries:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return TaskResult(1, repr(e), args, None)


    def run_serial_tasks(self, name, func, input_list):
        """
        Run tasks in serial (locally), given a function and list of inputs
        This will always return a list of TaskResults!
        """
        return [self.error_wrapper(func, i) for i in input_list]


    def run_concurrent_tasks(self, name, func, input_list, force_sequential):
        """
        Run tasks concurrently (locally), given a function a list of inputs
        This will always return a list of TaskResults!
        """
        pool_size = 1 if force_sequential else 10
        with multiprocessing.Pool(pool_size) as pool:
            results = pool.starmap(self.error_wrapper, [(func, i) for i in input_list], chunksize=self.chunksize)
        return results


    def run_prefect_tasks(self, name, func, input_list, force_sequential):
        """
        Run tasks using Prefect, using whichever task runner decided in self.run()
        This will always return a list of TaskResults!
        """

        from prefect import task

        task_wrapper = task(func, name=name, retries=self.retries, retry_delay_seconds=self.retry_delay)

        futures = []
        for i in input_list:
            w = [i[1] for i in futures] if force_sequential else None
            futures.append((i, task_wrapper.submit(*i, wait_for=w, return_state=False)))

        results = []

        for inputs, future in futures:
            state = future.wait(timeout=None)
            if state.is_completed():
                results.append(TaskResult(0, "Success", inputs, state.result()))
            elif state.is_failed() or state.is_crashed():
                try:
                    msg = repr(state.result(raise_on_failure=False))
                except:
                    msg = "Unable to retrieve error message"
                results.append(TaskResult(1, msg, inputs, None))
            else:
                pass

        # while futures:
        #     for ix, (inputs, future) in enumerate(futures):
        #         state = future.get_state()
        #         print(repr(state))
        #         print(repr(future))
        #         if state.is_completed():
        #             print('complete', ix, inputs)
        #             results.append(TaskResult(0, "Success", inputs, future.result()))
        #         elif state.is_failed() or state.is_crashed() or state.is_cancelled():
        #             print('fail', ix, inputs)
        #             try:
        #                 msg = repr(future.result(raise_on_failure=True))
        #             except:
        #                 msg = "Unable to retrieve error message"
        #             results.append(TaskResult(1, msg, inputs, None))
        #         else:
        #             print('not ready', ix, inputs)
        #             continue
        #         _ = futures.pop(ix)
        #         future.release()
        #     time.sleep(15)

        return results


    def run_mpi_tasks(self, name, func, input_list, force_sequential):
        """
        Run tasks using MPI, requiring the use of `mpirun`
        self.pool is an MPIPoolExecutor initialized by self.run()
        This will always return a list of TaskResults!
        """
        from mpi4py.futures import MPIPoolExecutor
        with MPIPoolExecutor(max_workers=self.mpi_max_workers, chunksize=self.chunksize) as pool:
            futures = []
            for i in input_list:
                f = pool.submit(self.error_wrapper, func, i)
                if force_sequential:
                    wait([f])
                futures.append(f)
        return [f.result() for f in futures]


    def run_tasks(self,
                  func,
                  input_list,
                  allow_futures: bool=True,
                  name: Optional[str]=None,
                  retries: Optional[int]=3,
                  retry_delay: Optional[int]=60,
                  force_sequential: bool=False):
        """
        Run a bunch of tasks, calling one of the above run_tasks functions
        This is the function that should be called most often from self.main()
        It will return a ResultTuple of TaskResults
        """

        timestamp = datetime.today()

        if not callable(func):
            raise TypeError("Function passed to run_tasks is not callable")

        # Save global retry settings, and override with current values
        old_retries, old_retry_delay = self.retries, self.retry_delay
        self.retries, self.retry_delay = self.init_retries(retries, retry_delay)

        logger = self.get_logger()

        if name is None:
            try:
                name = func.__name__
            except AttributeError:
                logger.warning("No name given for task run, and function does not have a name (multiple unnamed functions may result in log files being overwritten)")
                name = "unnamed"
        elif not isinstance(name, str):
            raise TypeError("Name of task run must be a string")

        if self.backend == "serial":
            results = self.run_serial_tasks(name, func, input_list)
        elif self.backend == "concurrent":
            results = self.run_concurrent_tasks(name, func, input_list, force_sequential)
        elif self.backend == "prefect":
            results = self.run_prefect_tasks(name, func, input_list, force_sequential)
        elif self.backend == "mpi":
            results = self.run_mpi_tasks(name, func, input_list, force_sequential)
        else:
            raise ValueError("Requested backend not recognized. Have you called this Dataset's run function?")

        if len(results) == 0:
            raise ValueError(f"Task run {name} yielded no results. Did it receive any inputs?")

        success_count = sum(1 for r in results if r.status_code == 0)
        error_count = len(results) - success_count
        if error_count == 0:
            logger.info(f"Task run {name} completed with {success_count} successes and no errors")
        else:
            logger.warning(f"Task run {name} completed with {error_count} errors and {success_count} successes")

        # Restore global retry settings
        self.retries, self.retry_delay = old_retries, old_retry_delay

        return ResultTuple(results, name, timestamp)


    def log_run(self,
                results,
                expand_args: list=[],
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

        fieldnames = ["status_code", "status_message"]

        should_expand_args = False
        args_expansion_spec = []

        for ai, ax in enumerate(expand_args):
            if ax is not None:
                should_expand_args = True
                fieldnames.append(ax)
                args_expansion_spec.append((ax, ai))

        if not should_expand_args:
            fieldnames.append("args")

        should_expand_results = False
        results_expansion_spec = []

        for ri, rx in enumerate(expand_results):
            if rx is not None:
                should_expand_results = True
                fieldnames.append(rx)
                results_expansion_spec.append((rx, ri))

        if not should_expand_results:
            fieldnames.append("results")

        rows_to_write = []

        for r in results:
            row = [r[0], r[1]]
            if should_expand_args:
                row.extend([r[2][i] if r[2] is not None else None for _, i in args_expansion_spec])
            else:
                row.append(r[2])

            if should_expand_results:
                row.extend([r[3][i] if r[3] is not None else None  for _, i in results_expansion_spec])
            else:
                row.append(r[3])

            rows_to_write.append(row)

        with open(log_file, "w", newline="") as lf:
            writer = csv.writer(lf)
            writer.writerow(fieldnames)
            writer.writerows(rows_to_write)


    def init_retries(self, retries: int, retry_delay: int, save_settings: bool=False):
        """
        Given a number of task retries and a retry_delay,
        checks to make sure those values are valid
        (ints greater than or equal to zero), and
        optionally sets class variables to keep their
        settings
        """
        if isinstance(retries, int):
            if retries < 0:
                raise ValueError("Number of task retries must be greater than or equal to zero")
            elif save_settings:
                self.retries = retries
        elif retries is None:
            retries = self.retries
        else:
            raise TypeError("retries must be an int greater than or equal to zero")

        if isinstance(retry_delay, int):
            if retry_delay < 0:
                raise ValueError("Retry delay must be greater than or equal to zero")
            elif save_settings:
                self.retry_delay = retry_delay
        elif retry_delay is None:
            retry_delay = self.retry_delay
        else:
            raise TypeError("retry_delay must be an int greater than or equal to zero, representing the number of seconds to wait before retrying a task")

        return retries, retry_delay


    def run(
        self,
        backend: Optional[str]=None,
        task_runner: Optional[str]=None,
        run_parallel: bool=False,
        max_workers: Optional[int]=None,
        # cores_per_process: Optional[int]=None,
        chunksize: int=1,
        log_dir: str="logs",
        logger_level=logging.INFO,
        retries: int=3,
        retry_delay: int=5,
        **kwargs):
        """
        Run a dataset
        Calls self.main() with a backend e.g. "prefect"
        This is how Datasets should usually be run
        """

        self.init_retries(retries, retry_delay, save_settings=True)

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
            elif task_runner == "concurrent" or task_runner is None:
                tr = ConcurrentTaskRunner
            elif task_runner == "dask":
                from prefect_dask import DaskTaskRunner
                if "cluster" in kwargs:
                    del kwargs["cluster"]
                if "cluster_kwargs" in kwargs:
                    del kwargs["cluster_kwargs"]
                tr = DaskTaskRunner(**kwargs)
            elif task_runner == "hpc":
                from hpc import HPCDaskTaskRunner
                job_name = "".join(self.name.split())
                tr = HPCDaskTaskRunner(num_procs=max_workers, job_name=job_name, log_dir=self.log_dir, **kwargs)
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
                from mpi4py import MPI
                comm = MPI.COMM_WORLD
                rank = comm.Get_rank()
                if rank != 0:
                    return

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
