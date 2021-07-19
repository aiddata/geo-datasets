"""
Download and unzip raw ESA Landcover data from Copernicus CDS

"""

import os
import glob
import pandas as pd
import numpy as np
from utility import run_tasks, get_current_timestamp, raster_calc


timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# -------------------------------------

# download directory
raw_dir = "/sciclone/aiddata10/REU/geo/raw/esa_landcover"

# final data directory
output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/esa_landcover"

# accepts int or str
years = range(1992, 2020)

mode = "parallel"

max_workers = 30

# -------------------------------------


input_dir = os.path.join(raw_dir, "uncompressed")

os.makedirs(output_dir, exist_ok=True)


mapping = {
    0: [0],
    10: [10, 11, 12],
    20: [20],
    30: [30, 40],
    50: [50, 60, 61, 62, 70, 71, 72, 80, 81, 82, 90, 100, 160, 170],
    110: [110, 130],
    120: [120, 121, 122],
    140: [140, 150, 151, 152, 153],
    180: [180],
    190: [190],
    200: [200, 201, 202],
    210: [210],
    220: [220]
}

vector_mapping = {vi:k for k,v in mapping.items() for vi in v}

map_func = np.vectorize(vector_mapping.get)

# more readable code to show what this mapping will do:
# for new_cat, old_cat in mapping.items():
#     data = np.where(data == old_cat, new_cat, data)



def run_lc_mapping(input_path, output_path):
    print("Processing: {0}".format(input_path))
    try:
        kwargs = {
            "driver": "GTiff",
            "compress": "LZW"
        }
        netcdf_path = f"netcdf:{input_path}:lccs_class"
        raster_calc(netcdf_path, output_path, map_func, **kwargs)
        return (0, "Success", output_path)
    except Exception as e:
        return (1, e, output_path)



if __name__ == '__main__':

    precheck = []

    for year in years:
        year_glob = glob.glob(os.path.join(input_dir, "*{}*.nc".format(year)))
        status, msg = 0, "Success"
        if len(year_glob) != 1:
            status, msg = 1, f"Multiple or no ({len(year_glob)}) files found"
            year_glob = [None]
        precheck.append((year, year_glob[0], status, msg))

    df = pd.DataFrame(precheck, columns=['year', 'netcdf', 'precheck_status', 'precheck_msg'])

    df["output_path"] = df['year'].apply(lambda x: os.path.join(output_dir, "esa_lc_{}.tif".format(x)))

    qlist = list(zip(df.loc[df['precheck_status'] == 0, 'netcdf'], df.loc[df['precheck_status'] == 0, 'output_path']))



    results = run_tasks(run_lc_mapping, qlist, mode, max_workers)



    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "output_path"])
    output_df = df.merge(results_df, on="output_path", how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    for ix, row in errors_df.iterrows():
        print(row)

    # output results to csv
    output_path = os.path.join(raw_dir, f"prepare_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)

