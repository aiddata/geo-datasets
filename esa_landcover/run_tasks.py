import os
import warnings


cluster_kwargs = {
    "name": "geo:esa",
    "shebang": "#!/bin/tcsh",
    "resource_spec": "nodes=1:c18a:ppn=12",
    "walltime": "01:00:00",
    "cores": 1,
    "processes": 1,
    "memory": "26GB",
    "interface": "ib0",
    "job_script_prologue": [
        "module load anaconda3/2021.05",
        "conda activate " + os.environ["CONDA_DEFAULT_ENV"],
        "cd " + os.getcwd(),
    ]
    # "job_extra_directives": ["-j oe"],
}

adapt_kwargs = {
    "minimum": 16,
    "maximum": 16,
}


def run_tasks(task_func, task_list, backend=None, run_parallel=False, add_error_wrapper=False, **kwargs):

    if backend == "prefect":
        results = run_prefect_tasks(task_func, task_list, run_parallel, add_error_wrapper, **kwargs)

    elif backend == "mpi":
        results = run_mpi_tasks(task_func, task_list, add_error_wrapper, **kwargs)

    elif run_parallel:
        results = run_multiprocessing_tasks(task_func, task_list, add_error_wrapper, **kwargs)

    else:
        results = run_standard_tasks(task_func, task_list, add_error_wrapper)

    return results


def _error_wrapper(func, args):
    try:
        result = func(*args)
        return (0, "Success", result)
    except Exception as e:
        return (1, repr(e), None)


def _simple_wrapper(func, args):
    return func(*args)


def run_prefect_tasks(task_func, task_list, run_parallel=False, add_error_wrapper=False, cluster_kwargs=cluster_kwargs, adapt_kwargs=adapt_kwargs, retries=3, **kwargs):

    from prefect import task, flow
    from prefect.task_runners import SequentialTaskRunner, ConcurrentTaskRunner
    from prefect.context import get_run_context
    from prefect.exceptions import MissingResult

    # TODO: option to choose consecutive vs sequential
    prefect_task_runner = ConcurrentTaskRunner
    if not run_parallel:
        prefect_task_runner = SequentialTaskRunner

    if run_parallel:
        from prefect_dask import DaskTaskRunner

        if cluster_kwargs is None:
            prefect_task_runner = DaskTaskRunner

        else:
            from dask_jobqueue import PBSCluster

            dask_task_runner_kwargs = {
                "cluster_class": PBSCluster,
                "cluster_kwargs": cluster_kwargs,
                "adapt_kwargs": adapt_kwargs,
            }

            prefect_task_runner = DaskTaskRunner(**dask_task_runner_kwargs)


    if add_error_wrapper:
        @task(retries=retries)
        def prefect_task_wrapper(func, *args, **kwargs):
            ctx = get_run_context()
            run_count = ctx.task_run.run_count
            if run_count == retries:
                try:
                    return (0, "Success", func(*args, **kwargs))
                except Exception as e:
                    return (1, repr(e), None)
            else:
                return (0, "Success", func(*args, **kwargs))
    else:
        @task(retries=retries)
        def prefect_task_wrapper(func, *args, **kwargs):
            return func(*args, **kwargs)


    @flow(task_runner=prefect_task_runner)
    def build_prefect_flow(task_list):
        task_futures = []
        for i in task_list:
            task_futures.append(prefect_task_wrapper.submit(task_func, *i))
        tf_results = []
        while len(task_futures) > 0:
            for i, tf in enumerate(task_futures):
                try:
                    result = tf.result()
                except MissingResult:
                    pass
                else:
                    tf_results.append(result)
                    task_futures.pop(i)
        return tf_results

    results = build_prefect_flow(task_list)

    return results


def run_mpi_tasks(task_func, task_list, add_error_wrapper=False, max_workers=None, chunksize=1):

    # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
    from mpi4py.futures import MPIPoolExecutor

    if max_workers is None:
        if "OMPI_UNIVERSE_SIZE" not in os.environ:
            raise ValueError("Parallel set to True and mpi4py is installed but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")
        max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
        warnings.warn(f"Using MPI but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")

    with MPIPoolExecutor(max_workers=max_workers) as executor:
        if add_error_wrapper:
            wrapper_list = [(task_func, i) for i in task_list]
            results_gen = executor.starmap(_error_wrapper, wrapper_list, chunksize=chunksize)
        else:
            results_gen = executor.starmap(task_func, task_list, chunksize=chunksize)

    results = list(results_gen)

    return results


def run_multiprocessing_tasks(task_func, task_list, add_error_wrapper=False, max_workers=None, chunksize=1):
    # see: https://docs.python.org/3/library/concurrent.futures.html
    from concurrent.futures import ProcessPoolExecutor

    if max_workers is None:
        import multiprocessing
        max_workers = multiprocessing.cpu_count()
        warnings.warn(f"Using multiprocessing but max_workers not specified. Defaulting to CPU count ({max_workers})")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        wrapper_list = [(task_func, i) for i in task_list]
        if add_error_wrapper:
            results_gen = executor.map(_error_wrapper, *zip(*wrapper_list), chunksize=chunksize)
        else:
            results_gen = executor.map(_simple_wrapper, *zip(*wrapper_list), chunksize=chunksize)

    results = list(results_gen)

    return results


def run_standard_tasks(task_func, task_list, add_error_wrapper=False):
    results = []

    if add_error_wrapper:
        wrapper_list = [(task_func, i) for i in task_list]
        for i in wrapper_list:
            results.append(_error_wrapper(*i))
    else:
        for i in task_list:
            results.append(task_func(*i))

    return results
