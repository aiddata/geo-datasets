"""
Process monthly and annual VIIRS NTL data

Two output layers are produced for both monthly and annual data:
1) average masked NTL values
    - masking removed background lighting among other interference
    - background values are set to "-1.5" and must be set to "0" (along with any other erroneous negative values) to avoid issues when running zonal statistics
    - the monthly vcmcfg product (no stray light corrections) is used to reflect methodology used in generating annual data by data provider
2) binary indicating cloud free coverage
    - uses the CF_CVG (count of cloud free measurements per pixel) to create a binary layer indicating whether each pixel had at least one cloud free measurement


Notes:
- Any gzipped files have been extracted to GeoTiff format
- Layers are global so do not require mosaicing of tiles
- Monthly (notile) has a block shape of (256, 256)
- Annual has a block of (1, 86401)
- Output files are compressed using LZW compression, and tiled (same as input tiling)
- I explored using Cloug Optimized GeoTiff (COG) format for the output, but GTiff is the only format in rasterio which allows writing directly to disk
    + Writing directly to disk is critical when used in combination with windowed read/write to avoid running out of memory during paralell processing
    + Using GeoTiff, each process should not require more than about 1 GB of memory
"""


import os
import glob
from pathlib import Path
import rasterio
import numpy as np
import pandas as pd
from datetime import datetime


date_string = datetime.now().strftime("%Y-%m-%d-%H%M")


mode = "parallel"
# mode = "serial"

run_monthly = True
run_annual = True

max_workers = 95


# minimum cloud free measurements threshold
cf_minimum = 1

# must be ints
# years = [2017, 2018]
years = range(2012, 2021)

# -----------------------------------------------------------------------------




def run(input_path, output_path, fname):
    """Wrapper to manage error handling around raster_calc
    """
    kwargs = {
        "compress": "LZW",
        "tiled": True,
        "driver": "GTiff"
    }
    if "binary" in fname:
        kwargs["dtype"] = "uint8"
    if fname == "make_cf_binary":
        function = make_cf_binary
    elif fname == "remove_negative":
        function = remove_negative
    try:
        raster_calc(input_path, output_path, function, **kwargs)
        return (0, "Success", input_path, output_path)
    except Exception as e:
        return (1, e, input_path, output_path)


def raster_calc(input_path, output_path, function, **kwargs):
    """
    Calculate raster values using rasterio based on function provided

    :param input_path: input raster
    :param output_path: path to write output raster to
    :param function: function to apply to input raster values
    :param kwargs: additional meta args used to write output raster
    """
    with rasterio.open(input_path) as src:
        assert len(set(src.block_shapes)) == 1
        meta = src.meta.copy()
        meta.update(**kwargs)
        with rasterio.open(output_path, "w", **meta) as dst:
            for ji, window in src.block_windows(1):
                in_data = src.read(window=window)
                out_data = function(in_data)
                out_data = out_data.astype(meta["dtype"])
                dst.write(out_data, window=window)


def remove_negative(x):
    """
    remove negative values from array
    """
    return np.where(x > 0, x, 0)


def make_binary(x, threshold):
    """
    create binary array based on threshold value
    """
    return np.where(x >= threshold, 1, 0)


make_cf_binary = lambda x: make_binary(x, cf_minimum)


def build_work_items(input_path, temporal_type, data_type, years_accept):
    """Build tuple used to define work items for processing

    :param input_path: path to input raster
    :param temporal_type: "monthly" or "annual"
    :param data_type: "avg_masked" or "cf_cvg"
    :return: tuple of (input_path, output_path, data_type)
    """
    output_base_dir = "/sciclone/aiddata10/REU/geo/data/rasters/viirs/eogdata"
    p = Path(input_path)
    if temporal_type == "monthly_notile_vcmcfg":
        temporal_id = p.parents[1].name
        year = int(temporal_id[:4])
        output_path = os.path.join(output_base_dir, "monthly_notile/v10/vcmcfg", data_type, temporal_id + ".tif")
    elif temporal_type == "annual":
        temporal_id = p.parents[0].name
        year = int(temporal_id)
        output_path = os.path.join(output_base_dir, "annual/v20", data_type, temporal_id + ".tif")
    if data_type == "avg_masked":
        fname = "remove_negative"
    elif data_type == "cf_cvg":
        fname = "make_cf_binary"
    valid_year = year in years_accept
    return [(input_path, output_path, fname), valid_year]



if __name__ == "__main__":

    print("Building work list")
    work_list = []

    base_dir = "/sciclone/aiddata10/REU/geo/raw/viirs"

    # monthly data
    if run_monthly:
        monthly_avg_glob_str = os.path.join(base_dir, "eogdata/monthly_notile/v10", "*/*/vcmcfg/*.avg_rade9h.masked.tif")
        monthly_cloud_glob_str = os.path.join(base_dir, "eogdata/monthly_notile/v10", "*/*/vcmcfg/*.cf_cvg.tif")

        monthly_avg_glob = glob.glob(monthly_avg_glob_str)
        monthly_cloud_glob = glob.glob(monthly_cloud_glob_str)

        assert len(monthly_avg_glob) == len(monthly_cloud_glob)

        work_list.extend([build_work_items(i, "monthly_notile_vcmcfg", "avg_masked", years) for i in monthly_avg_glob])
        work_list.extend([build_work_items(i, "monthly_notile_vcmcfg", "cf_cvg", years) for i in monthly_cloud_glob])

    # annual data
    if run_annual:
        annual_avg_glob_str = os.path.join(base_dir, "eogdata_extract/annual/v20", "*/*.average_masked.tif")
        annual_cloud_glob_str = os.path.join(base_dir, "eogdata_extract/annual/v20", "*/*.cf_cvg.tif")

        # 2012 has two versions of data since the satellite started collecting in 2012-04
        # option 1: 201204-201212 [only data from 2012]
        # option 2: 201204-201303 [fill in missing months with data from 2013 to avoid skewing avg due to seasonality
        # we use option 2 so filter out option 1
        annual_filter = lambda x: "201204-201212" not in x

        annual_avg_glob = list(filter(annual_filter, glob.glob(annual_avg_glob_str)))
        annual_cloud_glob = list(filter(annual_filter, glob.glob(annual_cloud_glob_str)))

        assert len(annual_avg_glob) == len(annual_cloud_glob)

        work_list.extend([build_work_items(i, "annual", "avg_masked", years) for i in annual_avg_glob])
        work_list.extend([build_work_items(i, "annual", "cf_cvg", years) for i in annual_cloud_glob])


    valid_work_list = [i[0] for i in work_list if i[1]]

    work_df = pd.DataFrame(valid_work_list, columns=["input_path", "output_path", "data_type"])

    output_dirs = work_df["output_path"].apply(lambda x: os.path.dirname(x))
    for i in set(output_dirs):
        os.makedirs(i, exist_ok=True)

    print(f"Running tasks in {mode}")

    if mode == "parallel":

        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor

        with MPIPoolExecutor(max_workers=max_workers) as executor:
            results_gen = executor.starmap(run, valid_work_list, chunksize=1, mpi_info=[("map-by", "node")])

        results = list(results_gen)

    else:
        results = []
        for i in valid_work_list:
            results.append(run(i))


    print("Outputting results")

    results_df = pd.DataFrame(results, columns=['status','message', "input_path", "output_path"])

    output_df = work_df.merge(results_df, left_on="input_path", right_on="input_path")
    print(output_df)

    output_path = os.path.join(base_dir, f"processing_results_{date_string}.csv")
    output_df.to_csv(output_path, index=False, encoding="utf-8")

