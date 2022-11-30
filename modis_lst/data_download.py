"""


"""

import os
import requests
import pandas as pd

from utility import listFD, get_hdf_url, SessionWithHeaderRedirection, run_tasks, get_current_timestamp

root_url = "https://e4ftl01.cr.usgs.gov"

data_url = os.path.join(root_url, "MOLT/MOD11C3.006")

output_dir = "/sciclone/aiddata10/REU/geo/raw/MODIS/terra/MOLT/MOD11C3.006"


# test connection
test_request = requests.get(data_url)
test_request.raise_for_status()

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

os.makedirs(output_dir, exist_ok=True)


def file_exists(path):
    return os.path.isfile(path)


def download_file(url, local_filename, identifier):
    """download individual file using session created

    this needs to be a standalone function rather than a method
    of SessionWithHeaderRedirection because we need to be able
    to pass it to our mpi4py map function
    """
    overwrite = False
    if file_exists(local_filename) and not overwrite:
        return (0, "Exists", identifier)
    with session.get(url, stream=True) as r:
        try:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            return (0, "Success", identifier)
        except Exception as e:
            return (1, repr(e), identifier)


year_list = range(2000, 2021)

mode = "parallel"
# model = "serial"

max_workers = 40


# replace with your user credentials
username = "<YOUR USERNAME>"
password = "<YOUR PASSWORD>"

# create session with the user credentials that will be used to authenticate access to the data
session = SessionWithHeaderRedirection(username, password)
# Note: session can be serialized but because we are streaming the files it cannot
# release the connection pool until one file is completed. Instead we create a new
# session for each process to use on its own.


if __name__ == "__main__":

    print("Preparing data download")

    # Note: calling listFD outside of the main block (on every process)
    #       caused some connection issues. Did not debug issue further
    #       as there is no need for this to be outside of main.
    top_level_list = listFD(data_url)

    """
    Filters links based on:
    1) dirname within link matches url being searched (i.e., not a link to an external page, only subdirectories of current url)
    2) basename matches YYYY.MM.DD format
    3) YYYY in basename matches a year in year_list
    """
    top_level_list_filtered = []
    for i in top_level_list:
        # url is technically a directory and ends with "/" so need to take dirname
        parent = os.path.dirname(i)
        # run checks
        keep = os.path.dirname(parent) == data_url and len(os.path.basename(parent).split(".")) == 3 and int(os.path.basename(parent).split(".")[0]) in year_list
        if keep: top_level_list_filtered.append(i)


    df = pd.DataFrame({"top_level": top_level_list_filtered})

    # get full url for each hdf file
    df["hdf"] = df["top_level"].apply(lambda x: get_hdf_url(x))

    # get temporal string from url parent directory
    # convert from YYYY.MM.DD to YYYYMM
    df["temporal"] = df["top_level"].apply(lambda x: "".join(os.path.basename(os.path.dirname(x)).split(".")[0:2]))

    # use basename from url to create local filename
    df["output"] = df.apply(lambda x: os.path.join(output_dir, x["temporal"] + "_" + x["hdf"].split('/')[-1]), axis=1)

    # confirm HDF url was found for each temporal directory
    missing_files_count = sum(df['hdf'] == "Error")
    print(f"{missing_files_count} missing HDF files")

    # generate list of tasks to iterate over
    flist = list(zip(df["hdf"], df["output"], df["temporal"]))

    print("Running data download")

    results = run_tasks(download_file, flist, mode, max_workers=max_workers, chunksize=1)


    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "temporal"])
    output_df = df.merge(results_df, on="temporal", how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    for ix, row in errors_df.iterrows():
        print(row)

    # output results to csv
    output_path = os.path.join(output_dir, f"data_download_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
