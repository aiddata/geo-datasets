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
filename_template = "V5GL02.HybridPM25.Global.{YEAR}{FIRST_MONTH}-{YEAR}{LAST_MONTH}"
output_dir = PurePath(os.getcwd(), "output_data")

year_list = range(1998, 2021)

timestamp = get_current_timestamp("%Y_%m_%d_%H_%M")

# can be "mpi" or "prefect"
# any other value will run the project locally
backend = "prefect"

run_parallel = True

# this only applies if backend == "mpi"
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
    for year in year_list:
        filename = filename_template.format(YEAR = year, FIRST_MONTH = "01", LAST_MONTH = "12")
        input_path = input_dir / "Annual" / (filename + ".nc")
        if os.path.exists(input_path):
            input_path_list.append(input_path)
            output_path = output_dir / "Annual" / (filename + ".tif")
            output_path_list.append(output_path)
        else:
            warnings.warn(f"No annual data found for year {year}. Skipping...")

    # run monthly data
    # TODO: find a way to set each year's month range individually so if researcher wants different months for each year can adjust
    for year in year_list:
        for i in range(1, 13):
            month = str(i).zfill(2)
            filename = filename_template.format(YEAR = year, FIRST_MONTH = month, LAST_MONTH = month)
            input_path = input_dir / "Monthly" / (filename + ".nc")
            if os.path.exists(input_path):
                input_path_list.append(input_path)
                output_path = output_dir / "Monthly" / (filename + ".tif")
                output_path_list.append(output_path)
            else:
                warnings.warn(f"No monthly data found for year {year} month {month}. Skipping...")
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
