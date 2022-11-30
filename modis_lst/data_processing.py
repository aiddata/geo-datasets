"""
"""

import os
from affine import Affine
import pandas as pd
import numpy as np

from utility import export_raster, load_hdf, get_temporal, get_current_timestamp, run_tasks


def process_hdf(input_path, layer, output_path, identifier):
    try:
        data = load_hdf(input_path, layer)
        # define the affine transformation
        #   5600m or 0.05 degree resolution
        #   global coverage
        transform = Affine(0.05, 0, -180,
                        0, -0.05, 90)
        meta = {"transform": transform, "nodata": 0, "height": data.shape[0], "width": data.shape[1]}
        # need to wrap data in array so it is 3-dimensions to account for raster band
        export_raster(np.array([data]), output_path, meta, quiet=True)
        return (0, "Success", identifier)
    except Exception as e:
        return (1, repr(e), identifier)




timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

input_dir = "/sciclone/aiddata10/REU/geo/raw/MODIS/terra/MOLT/MOD11C3.006"

output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/MODIS/terra/MOLT/MOD11C3.006"

mode = "parallel"

max_workers = 40


if __name__ == "__main__":

    print("Preparing data processing")

    raw_path_list = [
        os.path.join(input_dir, f) for f in os.listdir(input_dir)
        if f.endswith(".hdf")
    ]

    day_df = pd.DataFrame({"raw_path": raw_path_list})
    day_df["temporal"] = day_df["raw_path"].apply(lambda x: os.path.basename(x).split("_")[0])

    day_df["output"] = day_df["temporal"].apply(lambda x: os.path.join(output_dir, "monthly", "day", "modis_lst_day_cmg_" + x + ".tif"))
    day_df["layer"] = "LST_Day_CMG"

    night_df = day_df.copy(deep=True)

    night_df["output"] = night_df["temporal"].apply(lambda x: os.path.join(output_dir, "monthly", "night", "modis_lst_night_cmg_" + x + ".tif"))
    night_df["layer"] = "LST_Night_CMG"

    df = pd.concat([day_df, night_df], axis=0)


    output_dir_list = set([os.path.dirname(i) for i in df["output"]])

    for i in output_dir_list:
        os.makedirs(i, exist_ok=True)


    # generate list of tasks to iterate over
    flist = list(zip(df["raw_path"], df["layer"], df["output"], df["temporal"]))

    print("Running data processing")

    results = run_tasks(process_hdf, flist, mode, max_workers)


    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status","message", "temporal"])
    output_df = df.merge(results_df, on="temporal", how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    for ix, row in errors_df.iterrows():
        print(row)

    # output results to csv
    output_path = os.path.join(input_dir, f"data_processing_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
