
import os
import time
import datetime
import warnings
import time


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def _task_wrapper(func, args):
    try:
        result = func(*args)
        return result
    except Exception as e:
        return (1, repr(e), args, None)
        # TODO: check that it produces 4 cols and not throw error if remove None


def run_tasks(func, flist, parallel, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)
    wrapper_list = [(func, i) for i in flist]
    if parallel:
        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        # and: https://docs.python.org/3/library/concurrent.futures.html
        try:
            from mpi4py.futures import MPIPoolExecutor
            mpi = True
        except:
            from concurrent.futures import ProcessPoolExecutor
            mpi = False
        if max_workers is None:
            if mpi:
                if "OMPI_UNIVERSE_SIZE" not in os.environ:
                    raise ValueError("Parallel set to True and mpi4py is installed but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")
                max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
                warnings.warn(f"Parallel set to True (mpi4py is installed) but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")
            else:
                import multiprocessing
                max_workers = multiprocessing.cpu_count()
                warnings.warn(f"Parallel set to True (mpi4py is not installed) but max_workers not specified. Defaulting to CPU count ({max_workers})")
        if mpi:
            with MPIPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.starmap(_task_wrapper, wrapper_list, chunksize=chunksize)
        else:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.map(_task_wrapper, *zip(*wrapper_list), chunksize=chunksize)
        results = list(results_gen)
    else:
        results = []
        # for i in flist:
            # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))
    return results

