"""
for use with NDVI product from LTDR raw dataset

- Prepares list of all files
- Builds list of day files to process
- Processes day files
- Builds list of day files to aggregate to months
- Run month aggregation
- Builds list of month files to aggregate to years
- Run year aggregation

example LTDR product file names (ndvi product code is AVH13C1)

AVH13C1.A1981181.N07.004.2013227210959.hdf

split file name by "."
eg:

full file name - "AVH13C1.A1981181.N07.004.2013227210959.hdf"

0     product code        AVH13C1
1     date of image       A1981181
2     sensor code         N07
3     misc                004
4     processed date      2013227210959
5     extension           hdf

"""

import os
import errno
from collections import OrderedDict
from datetime import datetime

import rasterio
import numpy as np
import pandas as pd 
from osgeo import gdal, osr


mode = "auto"

try:
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
except:
    rank = 0


build_list = [
    # "daily",
    # "monthly",
    "yearly"
]


src_base = "/sciclone/aiddata10/REU/geo/raw/ltdr/LAADS/465"

dst_base = "/sciclone/aiddata10/REU/geo/data/rasters/ltdr/avhrr_ndvi_v5"


# filter options to accept/deny based on sensor, year
# all values must be strings
# do not enable/use both accept/deny for a given field

filter_options = {
    'use_sensor_accept': False,
    'sensor_accept': [],
    'use_sensor_deny': False,
    'sensor_deny': [],
    'use_year_accept': False,
    'year_accept': ['1987'],
    'use_year_deny': True,
    'year_deny': ['2019']
}


# -----------------------------------------------------------------------------


def build_data_list(input_base, ops):

    f = []
    for root, dirs, files in os.walk(input_base):
        for file in files:
            if file.endswith(".hdf"):
                f.append(os.path.join(root, file))

    df_dict_list = []
    for path in f:
        items = os.path.basename(path).split(".")
        year = items[1][1:5]
        day = items[1][5:8]
        sensor = items[2]
        month = "{0:02d}".format(
            datetime.strptime("{0}+{1}".format(year, day), "%Y+%j").month)
        df_dict_list.append({
            "path": path,
            "sensor": sensor,
            "year": year,
            "month": month,
            "day": day,
            "year_month": year+"_"+month,
            "year_day": year+"_"+day
        })

    df = pd.DataFrame(df_dict_list)
    df = df.sort(["path"])
    df = df.drop_duplicated("year_day", keep="last")

    sensors = sorted(list(set(df["sensor"])))
    years = sorted(list(set(df["year"])))

    filter_sensors = None
    if ops['use_sensor_accept']:
        filter_sensors = [i for i in sensors if i in ops['sensor_accept']]
    elif ops['use_sensor_deny']:
        filter_sensors = [i for i in sensors if i not in ops['sensor_deny']]

    if filter_sensors:
        df = df.loc[df["sensor"].isin(filter_sensors)]

    filter_years = None
    if ops['use_year_accept']:
        filter_years = [i for i in years if i in ops['year_accept']]
    elif ops['use_year_deny']:
        filter_years = [i for i in years if i not in ops['year_deny']]

    if filter_years:
        df = df.loc[df["year"].isin(filter_years)]

    return df


def prep_daily_data(task):
    src_file, output_base = task
    year = os.path.basename(src_file).split(".")[1][1:5]
    day = os.path.basename(src_file).split(".")[1][5:8]
    sensor = os.path.basename(src_file).split(".")[2]
    dst_dir = os.path.join(output_base, 'daily', year)
    make_dir(dst_dir)
    print "{0} {1} {2}".format(sensor, year, day)
    process_daily_data(src_file, dst_dir)


def prep_monthly_data(task):
    year, month, month_files, output_base = task

    data, meta = aggregate_rasters(file_list=month_files, method="max")
    month_path = os.path.join(output_base, 'monthly', year, "avhrr_ndvi_{0}_{1}.tif".format(year, month))
    write_raster(month_path, data, meta)


def prep_yearly_data(task):
    year, year_files, output_base = task

    data, meta = aggregate_rasters(file_list=year_files, method="mean")
    year_path = os.path.join(output_base, 'yearly', "avhrr_ndvi_{0}.tif".format(year))
    write_raster(year_path, data, meta)


df = build_data_list(src_base, filter_options)

day_qlist = []
for path in df["path"]:
    day_qlist.append([path, dst_base])

month_df = df[["path", "sensor", "year_month"]].groupby(["sensor", "year_month"], as_index=False).aggregate(lambda x: tuple(x))