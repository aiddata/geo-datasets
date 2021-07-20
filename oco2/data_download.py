"""


"""

import os
import requests
import pandas as pd

from utility import run_tasks, get_current_timestamp, file_exists, find_files


data_url = "https://oco2.gesdisc.eosdis.nasa.gov/data/OCO2_DATA/OCO2_L2_Lite_FP.10r/"

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# -------------------------------------

output_dir = "/sciclone/aiddata10/REU/geo/raw/gesdisc/OCO2_L2_Lite_FP_V10r/"

year_list = range(2015, 2021)

mode = "parallel"
# model = "serial"

max_workers = 35

# -------------------------------------


def download_file(url, local_filename):
    """download individual file using session created

    this needs to be a standalone function rather than a method
    of SessionWithHeaderRedirection because we need to be able
    to pass it to our mpi4py map function
    """
    overwrite = False
    if file_exists(local_filename) and not overwrite:
        return (0, "Exists", url)
    with requests.get(url, stream=True) as r:
        try:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            return (0, "Success", url)
        except Exception as e:
            return (1, repr(e), url)



# test connection
test_request = requests.get(data_url)
test_request.raise_for_status()


if __name__ == "__main__":

    print("Preparing data download")

    year_file_list = []
    for year in year_list:
        year_url = os.path.join(data_url, str(year))
        year_files = find_files(year_url, ".nc4")
        year_file_list.extend(year_files)


    df = pd.DataFrame({"raw_url": year_file_list})


    # use basename from url to create local filename
    df["output"] = df["raw_url"].apply(lambda x: os.path.join(output_dir, os.path.basename(x)))

    os.makedirs(output_dir, exist_ok=True)

    # generate list of tasks to iterate over
    flist = list(zip(df["raw_url"], df["output"]))


    print("Running data download")

    results = run_tasks(download_file, flist, mode, max_workers=max_workers, chunksize=1)

    # ---------
    # column name for join field in original df
    results_join_field_name = "raw_url"
    # position of join field in each tuple in task list
    results_join_field_loc = 0
    # ---------

    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", results_join_field_name])
    results_df[results_join_field_name] = results_df[results_join_field_name].apply(lambda x: x[results_join_field_loc])

    output_df = df.merge(results_df, on=results_join_field_name, how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    for ix, row in errors_df.iterrows():
        print(row)

    os.makedirs(os.path.join(output_dir, "results"), exist_ok=True)

    # output results to csv
    output_path = os.path.join(output_dir, "results", f"data_download_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
