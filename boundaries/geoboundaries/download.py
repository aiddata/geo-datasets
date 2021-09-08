"""
source /opt/anaconda3-2021.05/etc/profile.d/conda.csh

module unload gcc/4.7.3 python/2.7.8 openmpi-1.10.0-gcc mpi4py-2.0.0-gcc acml/5.3.1 numpy/1.9.2 gdal-nograss/1.11.2 proj/4.7.0 geos/3.5.0
module load gcc/9.3.0 openmpi/3.1.4/gcc-9.3.0 anaconda3/2021.05
unsetenv PYTHONPATH

# conda create -n geoboundaries -c conda-forge pandas requests
conda activate geoboundaries

unsetenv PYTHONPATH

requests
pandas
pymongo
fiona
rasterio
geopandas
shapely
numpy
rasterstats

"""

from pathlib import Path
import os
import requests
import time
import datetime
import json
import warnings
import pandas as pd


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def _task_wrapper(func, args):
    try:
        result = func(*args)
        return (0, "Success", args, result)
    except Exception as e:
        # raise
        return (1, repr(e), args, None)


def run_tasks(func, flist, parallel, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)
    wrapper_list = [(func, i) for i in flist]
    if parallel:
        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        # and: https://docs.python.org/3/library/concurrent.futures.html
        try:
            from mpi4py.futures import MPIPoolExecutor
            mpi = True
        except:
            from concurrent.futures import ProcessPoolExecutor
            mpi = False
        if max_workers is None:
            if mpi:
                if "OMPI_UNIVERSE_SIZE" not in os.environ:
                    raise ValueError("Parallel set to True and mpi4py is installed but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")
                max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
                warnings.warn(f"Parallel set to True (mpi4py is installed) but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")
            else:
                import multiprocessing
                max_workers = multiprocessing.cpu_count()
                warnings.warn(f"Parallel set to True (mpi4py is not installed) but max_workers not specified. Defaulting to CPU count ({max_workers})")
        if mpi:
            with MPIPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.starmap(_task_wrapper, wrapper_list, chunksize=chunksize)
        else:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.map(_task_wrapper, *zip(*wrapper_list), chunksize=chunksize)
        results = list(results_gen)
    else:
        results = []
        # for i in flist:
            # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))
    return results


def process(meta, output_base_dir, version, date_str):
    print("Downloading {}".format(meta["boundaryID"]))
    # file prep
    boundary_basename = "{}_{}".format(meta["boundaryISO"], meta["boundaryType"])
    boundary_dir = output_base_dir / boundary_basename
    os.makedirs(boundary_dir, exist_ok=True)
    # metadata
    # add info to meta (version, download date, etc)
    meta['gq_version'] = version
    meta['gq_download_date'] = date_str
    # save metadata as json
    meta_file = open(boundary_dir / "metadata.json", "w")
    json.dump(meta, meta_file, indent=4)
    meta_file.close()
    # boundary
    # get main geojson download path
    dlPath = meta['gjDownloadURL']
    # all features for boundary files
    try:
        geoBoundary = requests.get(dlPath).json()
    except:
        original_str = "raw.githubusercontent.com"
        replacement_str = "media.githubusercontent.com/media"
        lfs_dlPath = dlPath.replace(original_str, replacement_str)
        geoBoundary = requests.get(lfs_dlPath).json()
    # save geojson
    fname = "{}.geojson".format(boundary_basename)
    geo_file = open(boundary_dir / fname, "w")
    json.dump(geoBoundary, geo_file)
    geo_file.close()

if __name__ == '__main__':

version = 'v4'

# format for release data
base_api_url = "https://www.geoboundaries.org/api/{}/gbOpen/ALL/ALL/".format(version)

base_dir = Path("/sciclone/aiddata10/REU/geo/data/boundaries")


date_str = get_current_timestamp('%Y_%m_%d')

output_base_dir = base_dir / "geoboundaries" / version

r = requests.get(base_api_url)

r_json = r.json()

source_df = pd.DataFrame(r_json)
source_df["output_base_dir"] = output_base_dir
source_df["version"] = version
source_df["date_str"] = date_str


flist = [(i, output_base_dir, version, date_str) for i in r_json]

results = run_tasks(process, flist, parallel=True)

results_join_field_name = "boundaryID"
results_df = pd.DataFrame(results, columns=["status", "message", results_join_field_name, "output"])
results_df[results_join_field_name] = results_df[results_join_field_name].apply(lambda x: x[0][results_join_field_name])


output_df = results_df.merge(source_df, on=results_join_field_name)
output_df_path = base_dir / "geoboundaries" / f"{version}-{date_str}-dl_results.csv"
output_df.to_csv(output_df_path, index=False)
