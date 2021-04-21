"""
Python 3.8

Make sure to review all variables (static file names, years) when using this script for a new version of cru data



This script is fairly quick running in serial (<15minutes) but would be easy to adapt
for basic parellelization.

Suggested parallelization steps would be to just add the in_path variable for each
cru var to an instance of the band_temporal_list and combine all of the resulting lists.
The trios in this list could then each be passed to the extract_layer function using any
parellization map function.

"""

import os
import rasterio
import numpy as np

# this should match the dir names used for raw data and to be used for processed data
cru_label = "cru_ts_4.05"

start_year = 1901
end_year = 2020

years = range(start_year, end_year+1)
months = range(1, 13)

temporal_list = ["{}{}".format(y, str(m).zfill(2)) for y in years for m in months]

band_list = range(1, len(temporal_list)+1)

band_temporal_list = list(zip(band_list, temporal_list))

var_list = [
    # "cld",
    # "dtr",
    # "frs",
    # "pet",
    "pre",
    # "tmn",
    "tmp",
    # "tmx",
    # "vap",
    # "wet"
]



def extract_layer(input, out_path, band):
    """convert specified netcdf band to geotiff and output"""
    if isinstance(input, str):
        src = rasterio.open(input)
    elif isinstance(input, rasterio.io.DatasetReader):
        src = input
    else:
        raise ValueError("Invalid input type {}".format(type(input)))
    data = src.read(band)
    meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'dtype': src.meta["dtype"],
        'transform': src.meta["transform"],
        'driver': 'GTiff',
        'height': src.meta["height"],
        'width': src.meta["width"],
        'nodata': src.meta["nodata"],
        # 'compress': 'lzw'
    }
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(np.array([data]))



for var in var_list:
    print("Running variable:", var)
    var_dir = os.path.join("/sciclone/aiddata10/REU/geo/data/rasters", cru_label, "monthly", var)
    os.makedirs(var_dir, exist_ok=True)
    in_path = "netcdf:/sciclone/aiddata10/REU/geo/raw/{0}/cru_ts4.05.1901.2020.{1}.dat.nc:{1}".format(cru_label, var)
    src = rasterio.open(in_path)
    for band, temporal in band_temporal_list:
        print("\tprocessing", temporal)
        fname = "cru.{}.{}.tif".format(var, temporal)
        out_path = os.path.join(var_dir, fname)
        extract_layer(src, out_path, band )
    src.close()



# =============================================================================



def aggregate_rasters(file_list, method="mean"):
    """Aggregate multiple rasters

    Aggregates multiple rasters with same features (dimensions, transform,
    pixel size, etc.) and creates single layer using aggregation method
    specified.

    Supported methods: mean (default), max, min, sum

    Arguments
        file_list (list): list of file paths for rasters to be aggregated
        method (str): method used for aggregation

    Return
        result: rasterio Raster instance
    """
    store = None
    for ix, file_path in enumerate(file_list):
        try:
            raster = rasterio.open(file_path)
        except:
            print("Could not include file in aggregation ({0})".format(file_path))
            continue
        active = raster.read(masked=True)
        if store is None:
            store = active.copy()
        else:
            # make sure dimensions match
            if active.shape != store.shape:
                raise Exception("Dimensions of rasters do not match")
            if method == "max":
                store = np.ma.array((store, active)).max(axis=0)
                # non masked array alternatives
                # store = np.maximum.reduce([store, active])
                # store = np.vstack([store, active]).max(axis=0)
            elif method == "mean":
                if ix == 1:
                    weights = (~store.mask).astype(int)
                store = np.ma.average(np.ma.array((store, active)), axis=0, weights=[weights, (~active.mask).astype(int)])
                weights += (~active.mask).astype(int)
            elif method == "min":
                store = np.ma.array((store, active)).min(axis=0)
            elif method == "sum":
                store = np.ma.array((store, active)).sum(axis=0)
            else:
                raise Exception("Invalid method")
    store = store.filled(raster.nodata)
    return store, raster.profile



import multiprocessing

print(multiprocessing.cpu_count())


def run_yearly_data(year, method, var, cru_label):
    print("Running:", var, method, year)
    src_base = os.path.join("/sciclone/aiddata10/REU/geo/data/rasters", cru_label, "monthly", var)
    dst_base = os.path.join("/sciclone/aiddata10/REU/geo/data/rasters", cru_label, "yearly", var, method)
    year_files = sorted([os.path.join(src_base, i) for i in os.listdir(src_base) if "cru.{}.{}".format(var, year) in i])
    year_mask = "cru.{}.YYYY.tif".format(var)
    year_path = os.path.join(dst_base, year_mask.replace("YYYY", str(year)))
    # aggregate
    data, meta = aggregate_rasters(year_files, method)
    # write geotiff
    meta['dtype'] = data.dtype
    with rasterio.open(year_path, 'w', **meta) as result:
        result.write(data)


method_list = ["mean", "min", "max", "sum"]

qlist = []
for var in var_list:
    for method in method_list:
        dst_base = os.path.join("/sciclone/aiddata10/REU/geo/data/rasters", cru_label, "yearly", var, method)
        os.makedirs(dst_base, exist_ok=True)
        for year in years:
            qlist.append([year, method, var, cru_label])



with multiprocessing.Pool(processes=64) as pool:
    results = pool.starmap(run_yearly_data, qlist)
