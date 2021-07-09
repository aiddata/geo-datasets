"""

does not run well on hima nodes?

setenv OMPI_MCA_rmaps_base_oversubscribe yes

"""


import os
import glob
import tarfile
import gzip
import shutil
import pandas as pd
from datetime import datetime
from mpi4py import MPI


date_string = datetime.now().strftime("%Y-%m-%d-%H%M")


mode = "parallel"
# mode = "serial"

base_dir = "/sciclone/aiddata10/REU/geo/raw/viirs"

raw_dir = os.path.join(base_dir, "eogdata")
extract_dir = os.path.join(base_dir, "eogdata_extract")

# years = [2020]
years = range(2012,2021)

max_workers = 120


# -------------------------------------


def extract_tgz(data):
    ix, tgz_path, out_dir = data
    try:
        tar = tarfile.open(tgz_path, 'r:gz')
        tar.extractall(out_dir)
        return (0, "Success", ix)
    except Exception as e:
        return (1, e, ix)


# if __name__ == "__main__":

#     print("Running monthly (tiled)")

#     monthly_raw_dir = os.path.join(base_dir, "eogdata/monthly/v10")

#     monthly_files = []

#     for year in years:
#         monthly_dir = os.path.join(monthly_raw_dir, str(year), "*/vcmcfg/SVDNB_npp_*.tgz")
#         monthly_files.extend(glob.glob(monthly_dir))

#     monthly_df = pd.DataFrame({"tgz_files": monthly_files})

#     monthly_df["extract_dir"] = monthly_df.tgz_files.apply(lambda x: os.path.dirname(x).replace("eogdata", "eogdata_extract"))

#     for i in set(monthly_df.extract_dir):
#         os.makedirs(i, exist_ok=True)

#     monthly_flist = list(zip(monthly_df.index, monthly_df.tgz_files, monthly_df.extract_dir))


#     if mode == "parallel":
#         # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
#         from mpi4py.futures import MPIPoolExecutor

#         with MPIPoolExecutor(max_workers=max_workers) as executor:
#             monthly_results_gen = executor.map(extract_tgz, monthly_flist, chunksize=2)

#         monthly_results = list(monthly_results_gen)

#     elif mode == "serial":
#         monthly_results = []
#         for i in monthly_flist:
#             monthly_results.append(extract_tgz(i))


#     print("Outputting monthly results")

#     monthly_tmp_results_df = pd.DataFrame(monthly_results, columns=['status','message', "ix"]).set_index("ix")
#     monthly_final_results_df = monthly_df.merge(monthly_tmp_results_df, how='left', left_index=True, right_index=True)
#     print(monthly_final_results_df)

#     monthly_final_results_path = os.path.join(base_dir, f"monthly_extract_{date_string}.csv")
#     monthly_final_results_df.to_csv(monthly_final_results_path, index=False, encoding="utf-8")



# -------------------------------------

def extract_gz(data):
    ix, gz_path, out_path = data
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(out_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return (0, "Success", ix)
    except Exception as e:
        return (1, e, ix)


if __name__ == "__main__":

    print("Running annual")

    annual_raw_dir = os.path.join(base_dir, "eogdata/annual/v20")

    annual_files = []

    for year in years:
        annual_dir = os.path.join(annual_raw_dir, str(year), "VNL_v2_npp_*_global_*.tif.gz")
        annual_files.extend(glob.glob(annual_dir))

    annual_df = pd.DataFrame({"gz_files": annual_files})

    annual_df["extract_file"] = annual_df.gz_files.apply(lambda x: x[:-3].replace("eogdata", "eogdata_extract"))

    for i in set(annual_df.extract_file):
        os.makedirs(os.path.dirname(i), exist_ok=True)


    annual_flist = list(zip(annual_df.index, annual_df.gz_files, annual_df.extract_file))


    if mode == "parallel":

        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor

        with MPIPoolExecutor(max_workers=max_workers) as executor:
            annual_results_gen = executor.map(extract_gz, annual_flist, chunksize=2)

        annual_results = list(annual_results_gen)

    else:
        annual_results = []
        for i in annual_flist:
            annual_results.append(extract_gz(i))


    print("Outputting annual results")

    annual_tmp_results_df = pd.DataFrame(annual_results, columns=['status','message', "ix"]).set_index("ix")
    annual_final_results_df = annual_df.merge(annual_tmp_results_df, how='left', left_index=True, right_index=True)
    print(annual_final_results_df)

    annual_final_results_path = os.path.join(base_dir, f"annual_extract_{date_string}.csv")
    annual_final_results_df.to_csv(annual_final_results_path, index=False, encoding="utf-8")





