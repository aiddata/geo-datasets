# for converting MONTHLY pm data downloaded from https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25/folder/148055008434 
# version: multiple file download - HPC version, based off Dr. Goodman's script for converting nc file to tiff image, MONTHLY data

import os
import warnings
from pathlib import PurePath
import pandas as pd

from download import download_data
from utils import get_current_timestamp, convert_file
from run_tasks import run_tasks

# -------------------------------------

input_dir = PurePath(os.getcwd(), "input_data")
input_filename_template = "V5GL02.HybridPM25.Global.{YEAR}{MONTH}-{YEAR}{MONTH}.nc"

output_dir = PurePath(os.getcwd(), "output_data")
output_filename_template = "V5GL02.HybridPM25.Global.{YEAR}{MONTH}-{YEAR}{MONTH}.tif"

year_list = [1998, 2020]

timestamp = get_current_timestamp("%Y_%m_%d_%H_%M")

# can be "mpi" or "parallel"
# any other value will run the project locally
backend = None

run_parallel = False

max_workers = 4

# skip existing files while downloading?
skip_existing = True

# verify existing files' hashes while downloading?
# (skip_existing must be set to True above)
verify_existing = True

# -------------------------------------

def gen_task_list():
    
    # create output directories
    os.makedirs(output_dir / "Annual", exist_ok=True)
    os.makedirs(output_dir / "Monthly", exist_ok=True)

    input_path_list = []
    output_path_list = []
    
    # run annual data
    for f in os.listdir(input_dir / "Annual"):
        input_path_list.append(input_dir / "Annual" / f)
        output_path_list.append((output_dir / "Annual" / f).with_suffix(".tif"))

    # run monthly data
    # TODO: find a way to set each year's month range individually so if researcher wants different months for each year can adjust
    for year in year_list:
        for i in range(1, 12):
            month = str(i).zfill(2)
            input_path = input_filename_template.format(YEAR = year, MONTH = month)
            input_path_list.append(input_dir / "Monthly" / input_path)

            output_path = output_filename_template.format(YEAR = year, MONTH = month)
            output_path_list.append(output_dir / "Monthly" / output_path)
    df = pd.DataFrame({"input_file_path": input_path_list, "output_file_path": output_path_list})
    return df, list(zip(df["input_file_path"], df["output_file_path"]))

if __name__ == "__main__":

    print("Downloading / Verifying Data")

    download_data(skip_existing=skip_existing, verify_existing=verify_existing)

    print("Generating Task List")

    df, flist = gen_task_list()

    print("Running Data Conversion")

    results = run_tasks(task_list=flist, task_func=convert_file, backend=backend, run_parallel=run_parallel, max_workers=max_workers)

    print("Compiling Results")
    
    results_df = pd.DataFrame(results, columns=["output_file_path", "message", "status"])

    output_df = df.merge(results_df, on="output_file_path", how="left")
    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    os.makedirs(output_dir / "results", exist_ok=True)
    output_csv_path = output_dir / "results" / f"data_conversion_{timestamp}.csv"
    output_df.to_csv(output_csv_path, index=False)
