

from pathlib import Path

import pandas as pd

from utility import get_current_timestamp, run_tasks
from download import build_download_list, manage_download
from mosaic import run_convert, run_mosaic, build_convert_list, build_mosaic_list


################################################################################
# config options

# root of directory where data to be downloaded to
results_dir = Path('/home/userx/Desktop/black_marble_ntl')

# user earthdata app token
personal_app_token = "c2dvb2RtMDQ6YzJkdmIyUnRZVzVBWVdsa1pHRjBZUzUzYlM1bFpIVT06MTY2NTYwNzE4MzphNjhkOGFhOTBhZjZjMDE0YWM3NWExZjgyMTQ3Y2EzNDlkMzQ0NWJl"

# specify whether to prepare yearly or monthly data
# mode = 'monthly'
mode = 'yearly'

# specify years of data to prepare
# year_list = range(2012, 2023)
year_list = [2012]

#based on whether you want to run in parallel (True) or serial (False) processing of tasks
run_parallel = True

# max number of works to use for parallel processing
max_workers = 8

################################################################################

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')
(results_dir / 'logs').mkdir(parents=True, exist_ok=True)



print("Building download list...")

download_task_list = build_download_list(year_list, mode, results_dir)
download_task_list = [i + [personal_app_token] for i in download_task_list]

print("Running downloads...")

error_count = 1
while error_count > 0:

    results = run_tasks(manage_download, download_task_list, run_parallel, max_workers=max_workers, chunksize=1)
    results_df = pd.DataFrame(results, columns=["status", "message", "file_url", 'output_dest'])

    errors_df = results_df[results_df["status"] != 0]
    print("\t{} errors found out of {} download tasks".format(len(errors_df), len(results_df)))

    error_count = len(errors_df)


output_path = results_dir / 'logs' / f"data_download_{timestamp}.csv"
results_df.to_csv(output_path, index=False)



print("Running data convert")

convert_task_list = build_convert_list(year_list, mode, results_dir)

results = run_tasks(run_convert, convert_task_list, run_parallel, max_workers=max_workers, chunksize=1)
results_df = pd.DataFrame(results, columns=["status", "message", 'output_dest'])

errors_df = results_df[results_df["status"] != 0]
print("\t{} errors found out of {} convert tasks".format(len(errors_df), len(results_df)))

output_path = results_dir / 'logs' / f"data_convert_{timestamp}.csv"
results_df.to_csv(output_path, index=False)



print("Running data mosaic")

mosaic_task_list = build_mosaic_list(year_list, mode, results_dir)

results = run_tasks(run_mosaic, mosaic_task_list, run_parallel, max_workers=max_workers, chunksize=1)
results_df = pd.DataFrame(results, columns=["status", "message", "temporal", 'output_dest'])

errors_df = results_df[results_df["status"] != 0]
print("\t{} errors found out of {} mosaic tasks".format(len(errors_df), len(results_df)))

output_path = results_dir / 'logs' / f"data_mosaic_{timestamp}.csv"
results_df.to_csv(output_path, index=False)
