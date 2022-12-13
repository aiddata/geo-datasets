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
    "resource_spec": "nodes=1:c18a:ppn=12",
    "cores": 12,
    "processes": 12,
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
        raise NotImplementedError("Hima cluster not yet supported")
    else:
        raise ValueError("Cluster name not recognized")
    cluster_kwargs["name"] = job_name
    cluster_kwargs["walltime"] = walltime
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
            "adapt_kwargs": {"minimum": num_procs, "maximum": num_procs},
        }
        super().__init__(**dask_task_runner_kwargs)


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
        try:
            return TaskResult(0, "Success", args, func(*args))
        except Exception as e:
            return TaskResult(1, repr(e), args, None)


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

        futures = [task_wrapper.submit(self, func, i) for i in input_list]
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

        logger = self.get_logger()

        if not callable(func):
            raise TypeError("Function passed to run_tasks is not callable")

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
            elif task_runner == "concurrent" or task_runner is None:
                tr = ConcurrentTaskRunner
            elif task_runner == "dask":
                from prefect_dask import DaskTaskRunner
                tr = DaskTaskRunner(**kwargs)
            elif task_runner == "hpc":
                #from hpc import HPCDaskTaskRunner
                job_name = "".join(self.name.split())
                tr = HPCDaskTaskRunner(num_procs=max_workers, job_name=job_name, **kwargs)
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


"""
Download and prepare data from the Malaria Atlas Project

https://data.malariaatlas.org

Data is no longer directly available from a file server but is instead provided via a GeoServer instance.
This can still be retrieved using a standard request call but to determine the proper URL requires initiating the download via their website mapping platform and tracing the corresponding network call. This process only needs to be done once for new datasets; URLs for the datasets listed below have already been identifed.


Current download options:
- pf_incidence_rate: incidence data for Plasmodium falciparum species of malaria (rate per 1000 people)


Unless otherwise specified, all datasets are:
- global
- from 2000-2020
- downloaded as a single zip file containing each datasets (all data in zip root directory)
- single band LZW-compressed GeoTIFF files at 2.5 arcminute resolution
"""

import os
import sys
import shutil
import requests
from copy import copy
from pathlib import Path
from zipfile import ZipFile
from configparser import ConfigParser

import rasterio
from rasterio import windows

#sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
#sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')

#from dataset import Dataset


class MalariaAtlasProject(Dataset):
    name = "Malaria Atlas Project"

    def __init__(self, raw_dir, output_dir, years, dataset="pf_incidence_rate", overwrite_download=False, overwrite_processing=False):

        #sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.dataset = dataset
        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing

        dataset_lookup = {
            "pf_incidence_rate": {
                "data_zipFile_url": 'https://data.malariaatlas.org/geoserver/Malaria/ows?service=CSW&version=2.0.1&request=DirectDownload&ResourceId=Malaria:202206_Global_Pf_Incidence_Rate',
                "data_name": "202206_Global_Pf_Incidence_Rate"
            },
        }

        self.data_info = dataset_lookup[self.dataset]


    def test_connection(self):
        # test connection
        test_request = requests.get("https://data.malariaatlas.org", verify=True)
        test_request.raise_for_status()


    def copy_files(self, zip_path, zip_file, dst_path, cog_path):
        if not os.path.isfile(dst_path) or self.overwrite_processing:
            with ZipFile(zip_path) as myzip:
                with myzip.open(zip_file) as src:
                    with open(dst_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)

            if not os.path.isfile(dst_path):
                raise Exception("File extracted but not found at destination")

        return (dst_path, cog_path)


    def convert_to_cog(self, src_path, dst_path):
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
        """
        logger = self.get_logger()

        if not self.overwrite_processing and dst_path.exists():
            logger.info(f"COG Exists: {dst_path}")
            return

        logger.info(f"Generating COG: {dst_path}")

        with rasterio.open(src_path, 'r') as src:

            profile = copy(src.profile)

            profile.update({
                'driver': 'COG',
                'compress': 'LZW',
            })

            with rasterio.open(dst_path, 'w+', **profile) as dst:

                for ji, src_window in src.block_windows(1):
                    # convert relative input window location to relative output window location
                    # using real world coordinates (bounds)
                    src_bounds = windows.bounds(src_window, transform=src.profile["transform"])
                    dst_window = windows.from_bounds(*src_bounds, transform=dst.profile["transform"])
                    # round the values of dest_window as they can be float
                    dst_window = windows.Window(round(dst_window.col_off), round(dst_window.row_off), round(dst_window.width), round(dst_window.height))
                    # read data from source window
                    r = src.read(1, window=src_window)
                    # write data to output window
                    dst.write(r, 1, window=dst_window)


    def copy_data_files(self, zip_file_local_name):

        logger = self.get_logger()

        # create zipFile to check if data was properly downloaded
        try:
            dataZip = ZipFile(zip_file_local_name)
        except:
            logger.warning(f"Could not read downloaded zipfile: {zip_file_local_name}")
            raise

        raw_geotiff_dir = self.raw_dir / "geotiff" / self.dataset
        raw_geotiff_dir.mkdir(parents=True, exist_ok=True)

        # validate years for processing
        zip_years = sorted([int(i[-8:-4]) for i in dataZip.namelist() if i.endswith('.tif')])
        year_list = self.years
        years = [i for i in year_list if i in zip_years]
        if len(year_list) != len(years):
            missing_years = set(year_list).symmetric_difference(set(years))
            logger.warning(f"Years not found in downloaded data {missing_years}")

        flist = []
        for year in years:
            year_file_name = self.data_info["data_name"] + f"_{year}.tif"

            tif_path = raw_geotiff_dir / year_file_name
            cog_path = self.output_dir / self.dataset / year_file_name

            flist.append((zip_file_local_name, year_file_name, tif_path, cog_path))

        return flist


    def download_file(self, url, local_filename):
        """Download a file from url to local_filename
        Downloads in chunks
        """
        with requests.get(url, stream=True, verify=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)


    def manage_download(self, url, local_filename):
        """download individual file using session created
        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """
        logger = self.get_logger()
        print('md', sys.path)
        #sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')

        max_attempts = 5
        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {url}")
        else:
            attempts = 1
            while attempts <= max_attempts:
                try:
                    self.download_file(url, local_filename)
                except Exception as e:
                    attempts += 1
                    if attempts > max_attempts:
                        raise e
                else:
                    logger.info(f"Downloaded: {url}")
                    return

    def main(self):

        logger = self.get_logger()

        raw_zip_dir = self.raw_dir / "zip" / self.dataset
        raw_zip_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Testing Connection...")
        self.test_connection()
        #sys.path.insert(1, '/sciclone/home20/smgoodman/geo-datasets-testing/geo-datasets/global_scripts')

        logger.info("Running data download")
        zip_file_local_name = raw_zip_dir / (self.data_info["data_name"] + ".zip")
        # download data zipFile from url to the local output directory
        downloads = self.run_tasks(self.manage_download, [(self.data_info["data_zipFile_url"], zip_file_local_name)])
        self.log_run(downloads)

        logger.info("Copying data files")
        file_copy_list = self.copy_data_files(zip_file_local_name)
        copy_futures = self.run_tasks(self.copy_files, file_copy_list)
        self.log_run(copy_futures)

        (self.output_dir / self.dataset).mkdir(parents=True, exist_ok=True)

        logger.info("Converting raw tifs to COGs")
        conversions = self.run_tasks(self.convert_to_cog, copy_futures.results())
        self.log_run(conversions)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "dataset": config["main"]["dataset"],
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs"
    }


