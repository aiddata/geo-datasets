"""
"""

import os
import time
import datetime
import pandas as pd

from utility import aggregate_rasters, export_raster, run_tasks, get_current_timestamp



def run_yearly_data(year, year_files, method, out_path):
    try:
        data, meta = aggregate_rasters(file_list=year_files, method=method)
    except Exception as e:
        return(1, repr(e), year)
    try:
        export_raster(data, out_path, meta)
    except Exception as e:
        return(2, repr(e), year)
    return(0, "Success", year)


timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

mode = "parallel"

max_workers = 40

input_dir = "/sciclone/aiddata10/REU/geo/data/rasters/MODIS/terra/MOLT/MOD11C3.006"

method = "mean"


if __name__ == '__main__':

    print("Preparing data aggregation")

    src_dir = os.path.join(input_dir, "monthly")

    dst_dir = os.path.join(input_dir, "annual")

    data_list = []
    data_class_list = ["day", "night"]

    for data_class in data_class_list:
        month_files = [i for i in os.listdir(os.path.join(src_dir, data_class)) if i.endswith('.tif')]
        year_months = {}
        for mfile in month_files:
            # year associated with month
            myear = mfile.split("_")[-2]
            if myear not in year_months:
                year_months[myear] = list()
            year_months[myear].append(os.path.join(src_dir, data_class, mfile))
        data_list.extend([ (year_group, month_paths, data_class) for year_group, month_paths in year_months.items() ])

    df = pd.DataFrame(data_list, columns=['year', 'month_paths', 'data_class'])
    df["month_count"] = df["month_paths"].apply(lambda x: len(x))
    df["method"] = method
    df["output_path"] = df.apply(lambda x: os.path.join(dst_dir, x.data_class, x.method, f"modis_lst_{x.data_class}_cmg_YYYY.tif".replace("YYYY", str(x.year))), axis=1)


    output_dir_list = set([os.path.dirname(i) for i in df["output_path"]])

    for i in output_dir_list:
        os.makedirs(i, exist_ok=True)


    # generate list of tasks to iterate over
    flist = list(zip(df["year"], df["month_paths"], df["method"], df["output_path"]))


    print("Running data aggregation")

    results = run_tasks(run_yearly_data, flist, mode, max_workers)

    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "year"])
    output_df = df.merge(results_df, on="year", how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    for ix, row in errors_df.iterrows():
        print(row)

    # output results to csv
    output_path = os.path.join(input_dir, f"data_yearly_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
