"""
"""

import os
import pandas as pd

from utility import run_tasks, get_current_timestamp, file_exists, export_raster, create_mosaic


timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# -------------------------------------

version = "GFC-2020-v1.8"

input_dir = os.path.join("/sciclone/aiddata10/REU/geo/raw/global_forest_change", version)
output_dir = os.path.join("/sciclone/aiddata10/REU/geo/data/rasters/global_forest_change", version)

mode = "parallel"
# model = "serial"

max_workers = 4

# -------------------------------------


layers = [
    "treecover2000",
    "gain",
    "lossyear",
    "datamask",
]


def manage_mosaic(tile_list, output_path):
    if file_exists(output_path):
        return (0, "Exists", output_path)
    # data, meta = create_mosaic(tile_list)
    # export_raster(data, output_path, meta)
    create_mosaic(tile_list, output_path)


if __name__ == '__main__':

    print("Preparing data mosaic")

    df_list = []

    for layer in layers:
        # create output directory for each layer (data product)
        os.makedirs(os.path.join(output_dir, layer), exist_ok=True)
        # get list of tiles for layer
        layer_tile_list = [os.path.join(input_dir, layer, i) for i in os.listdir(os.path.join(input_dir, layer))]
        # add tuple to df list
        df_list.append( (layer, layer_tile_list) )


    # generate dataframe from list of tuples (layer, url)
    df = pd.DataFrame(df_list, columns=["layer", "tile_list"])

    # add output path for each tile to download
    df["output"] = df.apply(lambda x: os.path.join(output_dir, x["layer"], f'{x["layer"]}.tif'), axis=1)


    # generate list of tasks to iterate over
    flist = list(zip(df["tile_list"], df["output"]))


    print("Running data mosaic")

    results = run_tasks(manage_mosaic, flist, mode, max_workers=max_workers, chunksize=1)

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


    results_dir = os.path.join(input_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    # output results to csv
    output_path = os.path.join(results_dir, f"data_prepare_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
