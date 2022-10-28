"""
Download and prepare data from the Malaria Atlas Project

https://data.malariaatlas.org

Data is no longer directly available from a file server but is instead provided via a GeoServer instance.
This can still be retrieved using a standard request call but to determine the proper URL requires initiating the download via their website mapping platform and tracing the corresponding network call. This process only needs to be done once for new datasets; URLs for the datasets listed below have already been identifed.


Current download options:
- pf_incidence_rate: incidence data for Plasmodium falciparum species of malaria (rate per 1000 people)


Unless otherwise specified, all datasets are:
- global
- from 2000-2020
- downloaded as a single zip file containing each datasets (all data in zip root directory)
- single band LZW-compressed GeoTIFF files at 2.5 arcminute resolution


"""

import os
import requests
from zipfile import ZipFile
import warnings
from pathlib import Path

import pandas as pd

from utility import get_current_timestamp, manage_download, task
from run_tasks import run_tasks

# -------------------------------------

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

raw_data_base_dir = Path("/sciclone/aiddata10/REU/geo/raw/malaria_atlas_project")
processed_data_base_dir = Path("/sciclone/aiddata10/REU/geo/data/rasters/malaria_atlas_project")

# change var = if want to download a different variant's data
dataset = "pf_incidence_rate"

# change var = set to year range wanted
year_list = range(2000, 2021)

# change var: If want to change mode to serial need to change to False not "serial"
run_parallel = True

# change var: set max_workers to own max_workers
max_workers = 12

dataset_lookup = {
    "pf_incidence_rate": {
        "data_zipFile_url": 'https://data.malariaatlas.org/geoserver/Malaria/ows?service=CSW&version=2.0.1&request=DirectDownload&ResourceId=Malaria:202206_Global_Pf_Incidence_Rate',
        "data_name": "202206_Global_Pf_Incidence_Rate"
    },
}


# -------------------------------------

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

# test connection
test_request = requests.get("https://data.malariaatlas.org", verify=True)
test_request.raise_for_status()

zipFileLocalName = os.path.join(raw_data_zip_dir, data_info["data_name"] + ".zip")

# download data zipFile from url to the local output directory
manage_download(data_info["data_zipFile_url"], zipFileLocalName)

# create zipFile to check if data was properly downloaded
try:
    dataZip = ZipFile(zipFileLocalName)
except:
    print("Could not read downloaded zipfile")
    raise


print("Copying data files")

# validate years for processing
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
results = run_tasks(task, flist, backend=None, run_parallel=run_parallel, add_error_wrapper=True, max_workers=max_workers)



print("Results:")

# join download function results back to df
results_df = pd.DataFrame(results, columns=["status", "message", "cog_path"])

output_df = df.merge(results_df, on="cog_path", how="left")

errors_df = output_df[output_df["status"] != 0]
print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

output_path = log_dir / f"data_download_{timestamp}.csv"
output_df.to_csv(output_path, index=False)
