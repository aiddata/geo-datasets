import os
import copy
import time
import datetime
import warnings
import requests

def file_exists(path):
    return os.path.isfile(path)


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def download_file(url, local_filename):
    """Download a file from url to local_filename

    Downloads in chunks
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                f.write(chunk)


def _task_wrapper(func, args):
    try:
        func(*args)
        return (0, "Success", args)
    except Exception as e:
        return (1, repr(e), args)


def run_tasks(func, flist, mode, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)

    wrapper_list = [(func, i) for i in flist]

    if mode == "parallel":

        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor

        if max_workers is None:

            if "OMPI_UNIVERSE_SIZE" not in os.environ:
                raise ValueError("Mode set to parallel but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")

            max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
            warnings.warn(f"Mode set to parallel but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")

        with MPIPoolExecutor(max_workers=max_workers) as executor:
            # results_gen = executor.starmap(func, flist, chunksize=chunksize)
            results_gen = executor.starmap(_task_wrapper, wrapper_list, chunksize=chunksize)

        results = list(results_gen)

    else:
        results = []
        # for i in flist:
            # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))

    return results
