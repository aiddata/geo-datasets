# for converting MONTHLY pm data downloaded from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148055008434 
# version: multiple file download - HPC version, based off Dr. Goodman's script for converting nc file to tiff image, MONTHLY data

import warnings
import os
import pandas as pd

from utils import get_current_timestamp, export_raster, convert_file
from prefect_wrapper import convert_wrapper

# -------------------------------------
#CHANGE VAR
input_dir = "/home/jacob/Documents/geo-datasets/pm25/input_data/"
input_path_template = "V5GL02.HybridPM25.Global.{YEAR}{MONTH}-{YEAR}{MONTH}.nc"

#CONSIDER: Changing the output file name to something cleaner
#CHANGE VAR
output_dir = "/home/jacob/Documents/geo-datasets/pm25/output_data"
output_path_template = "V5GL02.HybridPM25.Global.{YEAR}{MONTH}-{YEAR}{MONTH}.tif"

#CHANGE VAR
year_list = range(2003, 2004)

#CHANGE VAR
month_list = range(1, 3)

timestamp = get_current_timestamp("%Y_%m_%d_%H_%M")

#CHANGE VAR: adjust based on whether you want parallel processing
run_parallel = False

#CHANGE VAR: adjust based on system's maximum workers
max_workers = 4

use_prefect = True

# -------------------------------------

if __name__ == "__main__":

    print("Preparing Data Conversion:")
    
    # create output directories
    os.makedirs(os.path.join(output_dir, "Monthly"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "Annual"), exist_ok=True)

    input_path_list = []
    output_path_list = []
    
    # run annual data
    for f in os.listdir(os.path.join(output_dir, "Annual")):
        input_path_list.append(os.path.join(output_dir, "Annual", f))
        output_path_list.append(os.path.join(output_dir, "Annual", f))

    # run monthly data
    # TODO: find a way to set each year's month range individually so if researcher wants different months for each year can adjust
    for year in year_list:
        for month in month_list:
            if month < 10:
                month = "0" + str(month)
            input_path = input_path_template.format(YEAR = year, MONTH = month)
            input_path_list.append(os.path.join(input_dir, "Monthly", input_path))

            output_path = output_path_template.format(YEAR = year, MONTH = month)
            output_path_list.append(os.path.join(output_dir, "Monthly", output_path))
    df = pd.DataFrame({"input_file_path": input_path_list, "output_file_path": output_path_list})
    flist = list(zip(df["input_file_path"], df["output_file_path"]))

    print("Running Data Conversion:")

    if use_prefect:
        # make sure prefect is available
        # TODO: handle error if prefect is not available
        from prefect import flow
        prefect_task_runner = None
        if run_parallel:
            from prefect_dask import DaskTaskRunner
            from dask_jobqueue import PBSCluster
            prefect_task_runner = DaskTaskRunner(**dask_task_runner_kwargs)
            cluster_kwargs = {
                "name": "ajh:ape",
                "shebang": "#!/bin/tcsh",
                "resource_spec": "nodes=1:c18a:ppn=12",
                "walltime": "00:20:00",
                "cores": 12,
                "processes": 12,
                "memory": "30GB",
                "interface": "ib0",
                "job_script_prologue": ["cd " + os.getcwd()]
                # "job_extra_directives": ["-j oe"],
            }

            adapt_kwargs = {
                "minimum": 12,
                "maximum": 12,
            }

            dask_task_runner_kwargs = {
                "cluster_class": PBSCluster,
                "cluster_kwargs": cluster_kwargs,
                "adapt_kwargs": adapt_kwargs,
            }
    
        @flow(task_runner=prefect_task_runner)
        def test_prefect_flow(flist):
            # TODO: submit all tasks, then cache results
            results = []
            for i in flist:
                results.append(convert_wrapper(*i))
            return results

        results = test_prefect_flow(flist)
    else:
        # run all downloads (parallel and serial options)
        if run_parallel:

            # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
            from mpi4py.futures import MPIPoolExecutor

            if max_workers is None:

                if "OMPI_UNIVERSE_SIZE" not in os.environ:
                    raise ValueError("Mode set to parallel but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")

                max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
                warnings.warn(f"Mode set to parallel but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")

            with MPIPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.starmap(convert_file, flist, chunksize=chunksize)

            results = list(results_gen)

        else:
            results = []
            for i in flist:
                results.append(convert_file(*i))

    print("Results:")
    
    results_df = pd.DataFrame(results, columns=["output_file_path", "message", "status"])

    output_df = df.merge(results_df, on="output_file_path", how="left")
    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    os.makedirs(os.path.join(output_dir, "results"), exist_ok=True)
    output_csv_path = os.path.join(output_dir, "results", f"data_conversion_{timestamp}.csv")
    output_df.to_csv(output_csv_path, index=False)
