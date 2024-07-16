import csv
import logging
import multiprocessing
import os
import shutil
import time
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import wait
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tempfile import mkdtemp, mkstemp
from typing import Any, Dict, Optional

from rio_cogeo import cog_validate

from .configuration import RunParameters

"""
A namedtuple that represents the results of one task
You can access a status code, for example, using TaskResult.status_code or TaskResult[0]
"""
TaskResult = namedtuple(
    "TaskResult", ["status_code", "status_message", "args", "result"]
)


class ResultTuple(Sequence):
    """
    This is an immutable sequence designed to hold TaskResults
    It also keeps track of the name of a run and the time it started
    ResultTuple.results() returns a list of results from each task
    """

    def __init__(
        self,
        iterable: Iterable[TaskResult],
        name: str,
        timestamp: datetime = datetime.today(),
    ):
        self.elements = []
        for value in iterable:
            if isinstance(value, TaskResult):
                self.elements.append(value)
            else:
                raise ValueError(
                    "ResultTuples must only consist of TaskResult namedtuples!"
                )
        self.name = name
        self.timestamp = timestamp

    def __getitem__(self, key: int):
        return self.elements[key]

    def __len__(self):
        return len(self.elements)

    def __repr__(self):
        success_count = sum(1 for t in self.elements if t.status_code == 0)
        error_count = len(self.elements) - success_count
        return f'<ResultTuple named "{self.name}" with {success_count} successes, {error_count} errors>'

    def args(self):
        args = [t.args for t in self.elements if t.status_code == 0]
        if len(args) < len(self.elements):
            logging.getLogger("dataset").warning(
                f"args() function for ResultTuple {self.name} skipping errored tasks"
            )
        return args

    def results(self):
        results = [t.result for t in self.elements if t.status_code == 0]
        if len(results) < len(self.elements):
            logging.getLogger("dataset").warning(
                f"results() function for ResultTuple {self.name} skipping errored tasks"
            )
        return results


class Dataset(ABC):
    """
    This is the base class for Datasets, providing functions that manage task runs and logs
    """

    backend: str
    name: str
    retries: int
    retry_delay: int

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

    @contextmanager
    def tmp_to_dst_file(
        self,
        final_dst: str | os.PathLike,
        make_dst_dir: bool = False,
        tmp_dir: Optional[str | os.PathLike] = None,
        validate_cog: bool = False,
    ):
        logger = self.get_logger()

        final_dst = Path(final_dst)

        # make sure that final_dst parent directory exists
        if not final_dst.parent.exists():
            if make_dst_dir:
                os.makedirs(final_dst.parent, exist_ok=True)
            else:
                raise FileNotFoundError(
                    f"Parent directory of requested filepath {str(final_dst)} does not exist."
                )

        tmp_sub_dir = mkdtemp(dir=tmp_dir)
        _, tmp_path = mkstemp(dir=tmp_sub_dir)
        logger.debug(
            f"Created temporary file {tmp_path} with final destination {str(final_dst)}"
        )
        yield tmp_path

        # validate Cloud Optimized GeoTIFF
        # doing this before move because disk r/w is almost certainly faster
        if validate_cog:
            is_valid, errors, warnings = cog_validate(tmp_path)
            if is_valid:
                logger.info(
                    f"Successfully validated output COG {tmp_path} (destined for {str(final_dst)}))"
                )
            else:
                logger.exception(
                    f"Failed to validate COG {tmp_path} (destined for {str(final_dst)})"
                )
            for error in errors:
                logger.error(f"Error encountered when validating COG: {error}")
            for warning in warnings:
                logger.warning(f"Warning encountered when validating COG: {warning}")

        # move file from tmp_path to final_dst
        try:
            logger.debug(f"Attempting to move {tmp_path} to {str(final_dst)}")
            shutil.move(tmp_path, final_dst)
        except Exception:
            logger.exception(
                f"Failed to transfer temporary file {tmp_path} to final destination {str(final_dst)}"
            )
        else:
            logger.debug(
                f"Successfully transferred {tmp_path} to final destination {str(final_dst)}"
            )

    def error_wrapper(self, func: Callable, args: Dict[str, Any]):
        """
        This is the wrapper that is used when running individual tasks
        It will always return a TaskResult!
        """
        logger = self.get_logger()

        for try_no in range(self.retries + 1):
            try:
                return TaskResult(0, "Success", args, func(*args))
            except Exception as e:
                if self.bypass_error_wrapper:
                    logger.info(
                        "Task failed with exception, and error wrapper bypass enabled. Raising..."
                    )
                    raise
                if try_no < self.retries:
                    logger.error(f"Task failed with exception (retrying): {repr(e)}")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    logger.error(f"Task failed with exception (giving up): {repr(e)}")
                    return TaskResult(1, repr(e), args, None)

    def run_serial_tasks(self, name, func: Callable, input_list: Iterable[Any]):
        """
        Run tasks in serial (locally), given a function and list of inputs
        This will always return a list of TaskResults!
        """
        logger = self.get_logger()
        logger.debug(f"run_serial_tasks - input_list: {input_list}")
        return [self.error_wrapper(func, i) for i in input_list]

    def run_concurrent_tasks(
        self,
        name: str,
        func: Callable,
        input_list: Iterable[Any],
        force_sequential: bool,
        max_workers: int = None,
    ):
        """
        Run tasks concurrently (locally), given a function a list of inputs
        This will always return a list of TaskResults!
        """
        if max_workers is None:
            max_workers = self.max_workers
        pool_size = 1 if force_sequential else max_workers
        with multiprocessing.Pool(pool_size) as pool:
            results = pool.starmap(
                self.error_wrapper,
                [(func, i) for i in input_list],
                chunksize=self.chunksize,
            )
        return results

    def run_prefect_tasks(
        self,
        name: str,
        func: Callable,
        input_list: Iterable[Any],
        force_sequential: bool,
    ):
        """
        Run tasks using Prefect, using whichever task runner decided in self.run()
        This will always return a list of TaskResults!
        """

        from prefect import task

        logger = self.get_logger()

        task_wrapper = task(
            func,
            name=name,
            retries=self.retries,
            retry_delay_seconds=self.retry_delay,
            persist_result=True,
        )

        futures = []
        for i in input_list:
            w = [i[1] for i in futures] if force_sequential else None
            futures.append((i, task_wrapper.submit(*i, wait_for=w, return_state=False)))

        results = []

        states = [(i[0], i[1].wait()) for i in futures]

        while states:
            for ix, (inputs, state) in enumerate(states):
                if state.is_completed():
                    # print('complete', ix, inputs)
                    logger.info(f"complete - {ix} - {inputs}")

                    results.append(TaskResult(0, "Success", inputs, state.result()))
                elif state.is_failed() or state.is_crashed() or state.is_cancelled():
                    # print('fail', ix, inputs)
                    logger.info(f"fail - {ix} - {inputs}")

                    try:
                        msg = repr(state.result(raise_on_failure=True))
                    except Exception as e:
                        msg = f"Unable to retrieve error message - {e}"
                    results.append(TaskResult(1, msg, inputs, None))
                else:
                    # print('not ready', ix, inputs)
                    continue
                _ = states.pop(ix)
            time.sleep(5)

        # for inputs, future in futures:
        #     state = future.wait(60*60*2)
        #     if state.is_completed():
        #         results.append(TaskResult(0, "Success", inputs, state.result()))
        #     elif state.is_failed() or state.is_crashed():
        #         try:
        #             msg = repr(state.result(raise_on_failure=False))
        #         except:
        #             msg = "Unable to retrieve error message"
        #         results.append(TaskResult(1, msg, inputs, None))
        #     else:
        #         pass

        # while futures:
        #     for ix, (inputs, future) in enumerate(futures):
        #         state = future.get_state()
        #         # print(repr(state))
        #         # print(repr(future))
        #         if state.is_completed():
        #             print('complete', ix, inputs)
        #             results.append(TaskResult(0, "Success", inputs, future.result()))
        #         elif state.is_failed() or state.is_crashed() or state.is_cancelled():
        #             print('fail', ix, inputs)
        #             try:
        #                 msg = repr(future.result(raise_on_failure=True))
        #             except Exception as e:
        #                 msg = f"Unable to retrieve error message - {e}"
        #             results.append(TaskResult(1, msg, inputs, None))
        #         else:
        #             # print('not ready', ix, inputs)
        #             continue
        #         _ = futures.pop(ix)
        #         # future.release()
        #     time.sleep(5)

        return results

    def run_mpi_tasks(
        self,
        name: str,
        func: Callable,
        input_list: Iterable[Any],
        force_sequential: bool,
        max_workers: int = None,
    ):
        """
        Run tasks using MPI, requiring the use of `mpirun`
        self.pool is an MPIPoolExecutor initialized by self.run()
        This will always return a list of TaskResults!
        """
        from mpi4py.futures import MPIPoolExecutor

        if not max_workers:
            max_workers = self.mpi_max_workers

        with MPIPoolExecutor(
            max_workers=max_workers, chunksize=self.chunksize
        ) as pool:
            futures = []
            for i in input_list:
                f = pool.submit(self.error_wrapper, func, i)
                if force_sequential:
                    wait([f])
                futures.append(f)
        return [f.result() for f in futures]

    def run_tasks(
        self,
        func: Callable,
        input_list: Iterable[Any],
        allow_futures: bool = True,
        name: Optional[str] = None,
        retries: int = 3,
        retry_delay: int = 60,
        force_sequential: bool = False,
        force_serial: bool = False,
        max_workers: Optional[int] = None,
        prefect_concurrency_tag: bool = None,
        prefect_concurrency_task_value: bool = None,
    ):
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
                logger.warning(
                    "No name given for task run, and function does not have a name (multiple unnamed functions may result in log files being overwritten)"
                )
                name = "unnamed"
        elif not isinstance(name, str):
            raise TypeError("Name of task run must be a string")

        # if max_workers is None:
        #     max_workers = self.max_workers

        if self.backend == "serial" or force_serial:
            results = self.run_serial_tasks(name, func, input_list)
        elif self.backend == "concurrent":
            results = self.run_concurrent_tasks(
                name, func, input_list, force_sequential, max_workers=max_workers
            )
        elif self.backend == "prefect":
            from prefect.concurrency.asyncio import concurrency
            with concurrency(prefect_concurrency_tag, occupy=prefect_concurrency_task_value):
                results = self.run_prefect_tasks(name, func, input_list, force_sequential)

        elif self.backend == "mpi":
            results = self.run_mpi_tasks(name, func, input_list, force_sequential, max_workers=max_workers)
        else:
            raise ValueError(
                "Requested backend not recognized. Have you called this Dataset's run function?"
            )

        if len(results) == 0:
            raise ValueError(
                f"Task run {name} yielded no results. Did it receive any inputs?"
            )

        success_count = sum(1 for r in results if r.status_code == 0)
        error_count = len(results) - success_count
        if error_count == 0:
            logger.info(
                f"Task run {name} completed with {success_count} successes and no errors"
            )
        else:
            logger.warning(
                f"Task run {name} completed with {error_count} errors and {success_count} successes"
            )

        # Restore global retry settings
        self.retries, self.retry_delay = old_retries, old_retry_delay

        return ResultTuple(results, name, timestamp)

    def log_run(
        self,
        results,
        expand_args: list = [],
        expand_results: list = [],
        time_format_str: str = "%Y_%m_%d_%H_%M",
    ):
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
                row.extend(
                    [
                        r[2][i] if r[2] is not None else None
                        for _, i in args_expansion_spec
                    ]
                )
            else:
                row.append(r[2])

            if should_expand_results:
                row.extend(
                    [
                        r[3][i] if r[3] is not None else None
                        for _, i in results_expansion_spec
                    ]
                )
            else:
                row.append(r[3])

            rows_to_write.append(row)

        with open(log_file, "w", newline="") as lf:
            writer = csv.writer(lf)
            writer.writerow(fieldnames)
            writer.writerows(rows_to_write)

    def init_retries(self, retries: int, retry_delay: int, save_settings: bool = False):
        """
        Given a number of task retries and a retry_delay,
        checks to make sure those values are valid
        (ints greater than or equal to zero), and
        optionally sets class variables to keep their
        settings
        """
        if isinstance(retries, int):
            if retries < 0:
                raise ValueError(
                    "Number of task retries must be greater than or equal to zero"
                )
            elif save_settings:
                self.retries = retries
        else:
            raise TypeError("retries must be an int greater than or equal to zero")

        if isinstance(retry_delay, int):
            if retry_delay < 0:
                raise ValueError("Retry delay must be greater than or equal to zero")
            elif save_settings:
                self.retry_delay = retry_delay
        else:
            raise TypeError(
                "retry_delay must be an int greater than or equal to zero, representing the number of seconds to wait before retrying a task"
            )

        return retries, retry_delay

    def _check_env_and_run(self):
        """
        Check if $TMPDIR is in /local, log warning if it is
        Then, run self.main()
        """
        logger = self.get_logger()

        try:
            # $TMPDIR is the default temporary directory that deployments use to store and execute code
            # is $TMPDIR set, and can we resolve it (find it on the filesystem)?
            tmp_dir = Path(os.environ["TMPDIR"]).resolve(strict=True)
        except KeyError:
            # KeyError if there is no such environment variable
            logger.warning("No $TMPDIR environment variable found!")
        except FileNotFoundError:
            # when we tried to resolve the path, the folder wasn't found on filesystem
            logger.warning("$TMPDIR path not found!")
        else:
            # /local points to local storage on W&M HPC
            slash_local = Path("/local").resolve()
            # is /local a parent dir of tmp_dir?
            for p in tmp_dir.parents:
                if p.resolve() == slash_local:
                    logger.warning(
                        "$TMPDIR in /local, deployments won't be accessible to compute nodes."
                    )

        # run the dataset (self.main() should be defined in child class instance)
        self.main()

    def run(
        self,
        params: RunParameters,
        **kwargs,
    ):
        """
        Run a dataset
        Initializes class variables and chosen backend
        This is how Datasets should usually be run
        Eventually calls _check_env_and_run(), starting dataset (see below)
        """

        # get current timestamp and initialize log directory
        timestamp = datetime.today()
        time_format_str: str = "%Y_%m_%d_%H_%M"
        time_str = timestamp.strftime(time_format_str)
        self.log_dir = Path(params.log_dir) / time_str
        os.makedirs(self.log_dir, exist_ok=True)

        self.init_retries(params.retries, params.retry_delay, save_settings=True)

        self.chunksize = params.chunksize

        self.bypass_error_wrapper = params.bypass_error_wrapper

        # Allow datasets to set their own default max_workers
        if params.max_workers is None and hasattr(self, "max_workers"):
            max_workers = self.max_workers
        else:
            max_workers = params.max_workers
            self.max_workers = max_workers

        # If dataset doesn't come with a name use its class name
        if not self.name:
            self.name = self._type()

        if params.backend == "prefect":
            self.backend = "prefect"

            from prefect import flow
            from prefect.task_runners import SequentialTaskRunner, ConcurrentTaskRunner#, ThreadPoolTaskRunner


            if params.task_runner == "sequential":
                tr = SequentialTaskRunner
            elif params.task_runner == "concurrent" or params.task_runner is None:
                # placeholder for actual ThreadPoolTaskRunner release in future Prefect versions
                # tr = ThreadPoolTaskRunner(max_workers=max_workers)
                tr = ConcurrentTaskRunner()

            elif params.task_runner == "dask":
                from prefect_dask import DaskTaskRunner

                # if "cluster" in kwargs:
                # del kwargs["cluster"]
                # if "cluster_kwargs" in kwargs:
                # del kwargs["cluster_kwargs"]

                dask_cluster_kwargs = {
                    "n_workers": max_workers,
                    "threads_per_worker": params.threads_per_worker,
                }
                tr = DaskTaskRunner(cluster_kwargs=dask_cluster_kwargs)
            elif params.task_runner == "hpc":
                from hpc import HPCDaskTaskRunner

                job_name = "".join(self.name.split())
                tr = HPCDaskTaskRunner(
                    num_procs=max_workers,
                    job_name=job_name,
                    log_dir=self.log_dir,
                    **kwargs,
                )
            elif params.task_runner == "kubernetes":
                from dask_kubernetes.operator import KubeCluster, make_cluster_spec
                from prefect_dask import DaskTaskRunner

                spec = make_cluster_spec(name="selector-example", n_workers=2)
                spec["spec"]["worker"]["spec"]["containers"][0][
                    "image"
                ] = "docker.io/jacobwhall/geodata-dask"
                spec["spec"]["worker"]["spec"]["containers"][0][
                    "imagePullPolicy"
                ] = "Always"
                spec["spec"]["worker"]["spec"]["containers"][0]["env"] = [
                    {
                        "name": "DATA_MANAGER_VERSION",
                        "value": os.environ["DATA_MANAGER_VERSION"],
                    }
                ]
                spec["spec"]["worker"]["spec"]["containers"][0]["volumeMounts"] = [
                    {"name": "sciclone", "mountPath": "/sciclone"}
                ]

                spec["spec"]["worker"]["spec"]["volumes"] = [
                    {
                        "name": "sciclone",
                        "persistentVolumeClaim": {"claimName": "nova-geodata-prod"},
                    }
                ]

                dask_task_runner_kwargs = {
                    "cluster_class": KubeCluster,
                    "cluster_kwargs": {
                        "custom_cluster_spec": spec,
                    },
                    "adapt_kwargs": {
                        "minimum": 1,
                        "maximum": max_workers,
                    },
                }
                tr = DaskTaskRunner(**dask_task_runner_kwargs)
            else:
                raise ValueError("Prefect task runner not recognized")

            @flow(task_runner=tr, name=self.name)
            def prefect_main_wrapper():
                self._check_env_and_run()

            prefect_main_wrapper()

        else:
            logger = logging.getLogger("dataset")
            logger.setLevel(params.logger_level)
            logger.addHandler(logging.StreamHandler())

            if params.backend == "mpi":
                from mpi4py import MPI

                comm = MPI.COMM_WORLD
                rank = comm.Get_rank()
                if rank != 0:
                    return

                self.backend = "mpi"
                self.mpi_max_workers = max_workers

                self._check_env_and_run()

            elif params.backend == "local":
                if params.run_parallel:
                    self.backend = "concurrent"
                else:
                    self.backend = "serial"
                self._check_env_and_run()

            else:
                raise ValueError(f"Backend {params.backend} not recognized.")
