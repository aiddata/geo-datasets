"""
"""

import os
import requests
import itertools
import pandas as pd

from utility import run_tasks, get_current_timestamp, file_exists, download_file


timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# -------------------------------------

version = "GFC-2020-v1.8"

output_dir = os.path.join("/sciclone/aiddata10/REU/geo/raw/global_forest_change", version)

mode = "parallel"
# model = "serial"

max_workers = 35

# -------------------------------------


base_url = os.path.join("https://storage.googleapis.com/earthenginepartners-hansen", version)

layers = [
    "treecover2000",
    "lossyear",
    "datamask",
]



def manage_download(url, local_filename):
    """download individual file using session created

    this needs to be a standalone function rather than a method
    of SessionWithHeaderRedirection because we need to be able
    to pass it to our mpi4py map function
    """
    overwrite = False
    if file_exists(local_filename) and not overwrite:
        return (0, "Exists", url)
    try:
        download_file(url, local_filename)
        return (0, "Success", url)
    except Exception as e:
        return (1, repr(e), url)




if __name__ == '__main__':

    df_list = []

    for layer in layers:
        # create output directory for each layer (data product)
        os.makedirs(os.path.join(output_dir, layer), exist_ok=True)
        # get list of tiles to download from source
        r = requests.get(os.path.join(base_url, f"{layer}.txt"))
        # confirm request was successful
        r.raise_for_status()
        # convert raw request text to list
        layer_dl_list = filter(None, r.text.split("\n"))
        # generate list tuples consisting of layer name and tile url
        df_list.extend(list(zip(itertools.repeat(layer), layer_dl_list)))


    # generate dataframe from list of tuples (layer, url)
    df = pd.DataFrame(df_list, columns=["layer", "url"])

    # add output path for each tile to download
    df["output"] = df.apply(lambda x: os.path.join(output_dir, x["layer"], os.path.basename(x["url"])), axis=1)



    # generate list of tasks to iterate over
    flist = list(zip(df["url"], df["output"]))


    print("Running data download")

    results = run_tasks(manage_download, flist, mode, max_workers=max_workers, chunksize=1)

    # ---------
    # column name for join field in original df
    results_join_field_name = "output"
    # position of join field in each tuple in task list
    results_join_field_loc = 1
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
