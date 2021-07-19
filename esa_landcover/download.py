"""
Download and unzip raw ESA Landcover data from Copernicus CDS

"""

import os
import cdsapi
import pandas as pd
import glob
import zipfile
from utility import run_tasks, get_current_timestamp

c = cdsapi.Client()

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

v207_years = range(1992, 2016)
v211_years = range(2016, 2020)

# -------------------------------------

# download directory
raw_dir = "/sciclone/aiddata10/REU/geo/raw/esa_landcover"

# accepts int or str
years = range(1992, 2020)

mode = "parallel"

max_workers = 30

# -------------------------------------


def download(version, year):
    overwrite = False
    dl_path = os.path.join(raw_dir, "compressed", f"{year}.zip")
    try:
        if len(glob.glob(dl_path)) == 0 or overwrite:
            dl_meta = {
                'variable': 'all',
                'format': 'zip',
                'version': version,
                'year': year,
            }
            c.retrieve('satellite-land-cover', dl_meta, dl_path)
    except Exception as e:
        return (1, e, year)
    try:
        zipfile_glob = glob.glob(dl_path)
        if len(zipfile_glob) != 1:
            raise Exception(f"Multiple or no ({len(zipfile_glob)}) zip file found for {year}")
        zipfile_path = zipfile_glob[0]
        print(f"Unzipping {zipfile_path}...")
        with zipfile.ZipFile(zipfile_path) as zf:
            netcdf_namelist = [i for i in zf.namelist() if i.endswith(".nc")]
            if len(netcdf_namelist) != 1:
                raise Exception(f"Multiple or no ({len(netcdf_namelist)}) net cdf files found in zip for {year}")
            if not os.path.isfile(os.path.join(raw_dir, "uncompressed", netcdf_namelist[0])) or overwrite:
                zf.extract(netcdf_namelist[0], os.path.join(raw_dir, "uncompressed"))
                print(f"Unzip complete: {zipfile_path}...")
            else:
                print(f"Unzip exists: {zipfile_path}...")
    except Exception as e:
        return (2, e, year)
    else:
        return (0, "Success", year)



if __name__ == "__main__":

    os.makedirs(os.path.join(raw_dir, "compressed"), exist_ok=True)
    os.makedirs(os.path.join(raw_dir, "uncompressed"), exist_ok=True)

    qlist = []

    for year in years:
        if year in v207_years:
            version = 'v2.0.7cds'
        elif year in v211_years:
            version = 'v2.1.1'
        else:
            raise Exception("Invalid year {}".format(year))
        qlist.append((version, year))

    df = pd.DataFrame(qlist, columns=[ 'version', 'year'])


    results = run_tasks(download, qlist, mode, max_workers)


    # join download function results back to df
    results_df = pd.DataFrame(results, columns=["status", "message", "year"])
    output_df = df.merge(results_df, on="year", how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

    for ix, row in errors_df.iterrows():
        print(row)

    # output results to csv
    output_path = os.path.join(raw_dir, f"download_{timestamp}.csv")
    output_df.to_csv(output_path, index=False)
