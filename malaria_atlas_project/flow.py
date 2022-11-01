
import os
from zipfile import ZipFile
import warnings
from pathlib import Path

import pandas as pd

from utility import get_current_timestamp, task, download, load_parameters
from run_tasks import run_tasks



def data_flow(dataset, year_list, raw_data_base_dir, processed_data_base_dir, backend, run_parallel, max_workers):

    raw_data_base_dir = Path(raw_data_base_dir)
    processed_data_base_dir = Path(processed_data_base_dir)

    timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

    dataset_lookup = {
        "pf_incidence_rate": {
            "data_zipFile_url": 'https://data.malariaatlas.org/geoserver/Malaria/ows?service=CSW&version=2.0.1&request=DirectDownload&ResourceId=Malaria:202206_Global_Pf_Incidence_Rate',
            "data_name": "202206_Global_Pf_Incidence_Rate"
        },
    }

    data_info = dataset_lookup[dataset]

    raw_data_zip_dir = raw_data_base_dir / "zip" / dataset
    raw_data_geotiff_dir = raw_data_base_dir / "geotiff" / dataset
    processed_data_dir = processed_data_base_dir / dataset
    log_dir = processed_data_dir / "logs"

    raw_data_zip_dir.mkdir(parents=True, exist_ok=True)
    raw_data_geotiff_dir.mkdir(parents=True, exist_ok=True)
    processed_data_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)




    print("Running data download")

    zipFileLocalName = raw_data_zip_dir / data_info["data_name"] + ".zip"

    # download data zipFile from url to the local output directory
    download(data_info["data_zipFile_url"], zipFileLocalName)




    print("Copying data files")

    # validate years for processing
    dataZip = ZipFile(zipFileLocalName)
    zip_years = sorted([int(i[-8:-4]) for i in dataZip.namelist() if i.endswith('.tif')])
    years = [i for i in year_list if i in zip_years]
    if len(year_list) != len(years):
        warnings.warn(f'Years specificed for processing which were not in downloaded data {set(year_list).symmetric_difference(set(years))}')

    df_list = []
    for year in years:
        year_file_name = data_info["data_name"] + f"_{year}.tif"
        item = {
            "zip_path": zipFileLocalName,
            "zip_file": year_file_name,
            "tif_path": raw_data_geotiff_dir / year_file_name,
            "cog_path": processed_data_base_dir / year_file_name
        }
        df_list.append(item)

    df = pd.DataFrame(df_list)

    # generate list of tasks to iterate over
    flist = list(zip(df["zip_path"], df["zip_file"], df["tif_path"], df["cog_path"]))



    # unzip data zipFile and copy the years wanted
    results = run_tasks(task, flist, backend=backend, run_parallel=run_parallel, add_error_wrapper=False, max_workers=max_workers)



    print("Results:")

    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "cog_path"])

    output_df = df.merge(results_df, on="cog_path", how="left")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    output_path = log_dir / f"data_download_{timestamp}.csv"
    output_df.to_csv(output_path, index=False)



if __name__ == "__main__":


    params = load_parameters()


    # flow parameters
    dataset = params["dataset"]
    raw_data_base_dir = params["raw_data_base_dir"]
    processed_data_base_dir = params["processed_data_base_dir"]
    year_list = params["year_list"]


    # deployment configs
    backend = params["backend"]
    run_parallel = params["run_parallel"]
    max_workers = params["max_workers"]


    data_flow(dataset, year_list, raw_data_base_dir, processed_data_base_dir, backend, run_parallel, max_workers)