# data download script for worldPop data for specific age and sex structures
"""
worldpop: https://www.worldpop.org/geodata/listing?id=65
"""

import os
import requests
import pandas as pd

from utility import run_tasks, get_current_timestamp, file_exists, download_file



template_url = "https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020/{YEAR}/0_Mosaicked/global_mosaic_1km/global_{SEX}_{AGE}_{YEAR}_1km.tif"
# "https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020/{YEAR}/0_Mosaicked/global_mosaic_1km/global_{SEX}_{AGE}_{YEAR}_1km.tif"


timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# -------------------------------------

output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/worldpop/pop_age_sex_structures/1km_mosaic/"
template_download_dir = "{SEX}_{AGE}"
# change var: set to own directory

year_list = range(2000, 2021)
# change var: restrict range to whatever needed, data available from 2000-2020

run_parallel = True
# change var: If want to change mode to serial need to change to False not "serial"

max_workers = 16
# change var: set max_workers to own max_workers

# -------------------------------------

sex_list =  ["f"]
# change var: change based on targeted sex group, full range: ["f", "m"]

age_list = [0, 1]
for k in range(5, 85, 5):
    age_list.append(k)
# change var: change based on targeted age group, full range: [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80], can delete 0, 1 from first initialization if not included


# -------------------------------------

def manage_download(url, local_filename):
    """download individual file using session created
    this needs to be a standalone function rather than a method
    of SessionWithHeaderRedirection because we need to be able
    to pass it to our mpi4py map function
    """
    overwrite = False
    max_attempts = 5
    if file_exists(local_filename) and not overwrite:
        return (0, "Exists", url)
    attempts = 1
    while attempts <= max_attempts:
        try:
            download_file(url, local_filename)
            return (0, "Downloaded", url)
        except Exception as e:
            attempts += 1
            if attempts > max_attempts:
                raise



# test connection
test_request = requests.get("https://data.worldpop.org/GIS/", verify=True)
test_request.raise_for_status()


if __name__ == "__main__":

    print("Preparing data download")

    os.makedirs(output_dir, exist_ok=True)

    download_file_list = []
    dir_list = []
    for sex in sex_list:
        for age in age_list:
            download_dir = template_download_dir.format(SEX = sex, AGE = age)
            os.makedirs(os.path.join(output_dir, download_dir), exist_ok=True)
            for year in year_list:
                final_url = template_url.format(SEX = sex, AGE = age, YEAR = year)
                download_file_list.append(final_url)
                dir_list.append(os.path.join(output_dir, download_dir, os.path.basename(final_url)))


    df = pd.DataFrame({"raw_url": download_file_list, "output": dir_list})

    # generate list of tasks to iterate over
    flist = list(zip(df["raw_url"], df["output"]))


    print("Running data download")

    results = run_tasks(manage_download, flist, run_parallel, max_workers=max_workers, chunksize=1)

    # ---------
    # column name for join field in original df
    results_join_field_name = "raw_url"
    # position of join field in each tuple in task list
    results_join_field_loc = 2

    # ---------

    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "args", results_join_field_name])
    results_df[results_join_field_name] = results_df[results_join_field_name].apply(lambda x: x[results_join_field_loc] if x is not None else x)

    output_df = df.merge(results_df, on=results_join_field_name, how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))


    os.makedirs(os.path.join(output_dir, "results"), exist_ok=True)
    output_path = os.path.join(output_dir, "results", f"data_download_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
