# download incidence data for Plasmodium falciparum species of malaria, data available 2000-2020, downloads as single-band LZW-compressed GeoTIFF files at 2.5 arcminute resolution
# download the zip file for all the data, then extract data for years wanted into the main directory
# link: https://malariaatlas.org/malaria-burden-data-download/

import os
import requests
import pandas as pd
from utility import run_tasks, get_current_timestamp, file_exists, download_file
from zipfile import ZipFile
import shutil

# -------------------------------------

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# change var = if want to download a different variant's data
data_zipFile_url = "https://malariaatlas.org/wp-content/uploads/2022-gbd2020/Pf_Incidence.zip"

template_base = "incidence_rate_median_Global_admin0_{YEAR}.tif"
final_template_url = "Global_Pf_Incidence_Rate_{YEAR}.tif"
# change var = set to local output directory
output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/malaria_data/1km_mosaic/"

# change var = set to year range wanted
year_list = range(2000, 2021)

# change var: If want to change mode to serial need to change to False not "serial"
run_parallel = True

# change var: set max_workers to own max_workers
max_workers = 12

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
            return (0, "Downloaded", url)
        except Exception as e:
            attempts += 1
            if attempts > max_attempts:
                raise e

def copy_files(data_zip, cur_loc, new_loc):
    overwrite = False
    if file_exists(new_loc) and not overwrite:
        return (new_loc)
    else:
        try: 
            with ZipFile(data_zip) as myzip:
                with myzip.open(cur_loc) as source:
                    with open(new_loc, "wb") as target:
                        shutil.copyfileobj(source, target)
            return (new_loc)
        except Exception as e:
            raise e
            
def test_file_extract(new_file_path):
    if not file_exists(new_file_path):
        raise Exception

# test connection
test_request = requests.get("https://malariaatlas.org/malaria-burden-data-download/", verify=True)
test_request.raise_for_status()

if __name__ == "__main__":

    print("Preparing data download")

    os.makedirs(output_dir, exist_ok=True)

    zipFileLocalName = os.path.join(output_dir, os.path.basename(data_zipFile_url))

    print("Running data download")

    # download data zipFile from url to the local output directory
    manage_download(data_zipFile_url, zipFileLocalName)

    # create zipFile for code to refer to and check to see if data zipFile was properly downloaded
    dataZip = ZipFile(os.path.join(output_dir, "Pf_Incidence.zip"))

    print("Copying data files")

    year_file_list = []
    output_loc_list = []
    helper_file_list = []
    for year in year_list:
        year_file_name = (os.path.join("Pf_Incidence", "Raster Data", "Pf_incidence_rate_median", template_base.replace("{YEAR}", str(year))))
        year_file_list.append(year_file_name)
        output_file_name = (os.path.join(output_dir, final_template_url.replace("{YEAR}", str(year))))
        output_loc_list.append(output_file_name)
        helper_file_list.append(zipFileLocalName)
    
    df = pd.DataFrame({"zipFile": helper_file_list, "zipfile_loc": year_file_list, "output_loc": output_loc_list})

    # generate list of tasks to iterate over
    flist = list(zip(df["zipFile"], df["zipfile_loc"], df["output_loc"]))

    # unzip data zipFile and copy the years wanted
    results = run_tasks(copy_files, flist, run_parallel, max_workers=max_workers, chunksize=1)
    
    # test if file extracted properly
    for filePath in output_loc_list:
        test_file_extract(filePath)

    # ---------

    print("Results:")

    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "output_loc"])

    output_df = df.merge(results_df, on="output_loc", how="left")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    os.makedirs(os.path.join(output_dir, "results"), exist_ok=True)
    output_path = os.path.join(output_dir, "results", f"data_download_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
