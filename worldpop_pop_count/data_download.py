"""

worldpop: https://www.worldpop.org/geodata/listing?id=64

"""

import os
import requests
import pandas as pd

from utility import run_tasks, get_current_timestamp, file_exists, download_file



template_url = "https://data.worldpop.org/GIS/Population/Global_2000_2020/{YEAR}/0_Mosaicked/ppp_{YEAR}_1km_Aggregated.tif"


timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# -------------------------------------

output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/worldpop/population_counts/1km_mosaic/"

year_list = range(2000, 2021)

mode = "parallel"
# model = "serial"

max_workers = 16

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
            return
        except Exception as e:
            attempts += 1
            if attempts > max_attempts:
                raise



# test connection
test_request = requests.get("https://data.worldpop.org/GIS/", verify=True)
test_request.raise_for_status()


if __name__ == "__main__":

    print("Preparing data download")

    year_file_list = []
    for year in year_list:
        year_url = template_url.replace("{YEAR}", str(year))
        year_file_list.append(year_url)


    df = pd.DataFrame({"raw_url": year_file_list})


    # use basename from url to create local filename
    df["output"] = df["raw_url"].apply(lambda x: os.path.join(output_dir, os.path.basename(x)))

    os.makedirs(output_dir, exist_ok=True)

    # generate list of tasks to iterate over
    flist = list(zip(df["raw_url"], df["output"]))


    print("Running data download")

    results = run_tasks(manage_download, flist, mode, max_workers=max_workers, chunksize=1)

    # ---------
    # column name for join field in original df
    results_join_field_name = "raw_url"
    # position of join field in each tuple in task list
    results_join_field_loc = 2
    # ---------

    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "args", results_join_field_name])
    results_df[results_join_field_name] = results_df[results_join_field_name].apply(lambda x: x[results_join_field_loc])

    output_df = df.merge(results_df, on=results_join_field_name, how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))


    os.makedirs(os.path.join(output_dir, "results"), exist_ok=True)

    # output results to csv
    output_path = os.path.join(output_dir, "results", f"data_download_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
