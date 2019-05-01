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
    "daily",
    # "monthly",
    # "yearly"
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


def build_data_list(input_base, output_base, ops):
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
        output_path = os.path.join(
            output_base, "daily/avhrr_ndvi_v5_{}_{}.tif".format(year, day)
        )
        df_dict_list.append({
            "path": path,
            "sensor": sensor,
            "year": year,
            "month": month,
            "day": day,
            "year_month": year+"_"+month,
            "year_day": year+"_"+day,
            "output_path": output_path
        })
    df = pd.DataFrame(df_dict_list)
    df = df.sort(["path"])
    # df = df.drop_duplicates(subset="year_day", take_last=True)
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
    src, dst = task
    year = os.path.basename(src).split(".")[1][1:5]
    day = os.path.basename(src).split(".")[1][5:8]
    sensor = os.path.basename(src).split(".")[2]
    print "Rank {} - Processing Day {} {} {}".format(rank, sensor, year, day)
    process_daily_data(src, dst)


def prep_monthly_data(task):
    year_month, month_files, month_path = task
    print "Rank {} - Processing Month {}".format(rank, year_month)
    data, meta = aggregate_rasters(file_list=month_files, method="max")
    write_raster(month_path, data, meta)


def prep_yearly_data(task):
    year, year_files, year_path = task
    print "Rank {} - Processing Year {}".format(rank, year)
    data, meta = aggregate_rasters(file_list=year_files, method="mean")
    write_raster(year_path, data, meta)


def process_daily_data(input_path, output_path):
    """Process input raster and create output in output directory

    Unpack NDVI subdataset from a HDF container
    Reproject to EPSG:4326
    Set values <0 (other than nodata) to 0
    Write to GeoTiff

    Parts of code pulled from:

    https://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
    https://gis.stackexchange.com/questions/42584/how-to-call-gdal-translate-from-python-code
    https://stackoverflow.com/questions/10454316/how-to-project-and-resample-a-grid-to-match-another-grid-with-gdal-python/10538634#10538634
    https://jgomezdans.github.io/gdal_notes/reprojection.html

    Notes:

    Rebuilding geotransform is not really necessary in this case but might
    be useful for future data prep scripts that can use this as startng point.

    """
    # open the dataset and subdataset
    hdf_ds = gdal.Open(input_path, gdal.GA_ReadOnly)

    subdataset = 0
    band_ds = gdal.Open(
        hdf_ds.GetSubDatasets()[subdataset][0], gdal.GA_ReadOnly)

    # clean data
    band_array = band_ds.ReadAsArray().astype(np.int16)

    # band_array[np.where(band_array < 0)] = -9999

    band_array[np.where((band_array < 0) & (band_array > -9999))] = 0
    band_array[np.where(band_array > 10000)] = 10000


    # prep projections and transformations
    src_proj = osr.SpatialReference()
    src_proj.ImportFromWkt(band_ds.GetProjection())

    dst_proj = osr.SpatialReference()
    dst_proj.ImportFromEPSG(4326)

    tx = osr.CoordinateTransformation(src_proj, dst_proj)

    src_gt = band_ds.GetGeoTransform()
    pixel_xsize = src_gt[1]
    pixel_ysize = abs(src_gt[5])

    # extents
    (ulx, uly, ulz ) = tx.TransformPoint(src_gt[0], src_gt[3])

    (lrx, lry, lrz ) = tx.TransformPoint(
        src_gt[0] + src_gt[1]*band_ds.RasterXSize,
        src_gt[3] + src_gt[5]*band_ds.RasterYSize)

    # new geotransform
    dst_gt = (ulx, pixel_xsize, src_gt[2],
               uly, src_gt[4], -pixel_ysize)

    # create new raster
    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.Create(
        output_path,
        int((lrx - ulx)/pixel_xsize),
        int((uly - lry)/pixel_ysize),
        1,
        gdal.GDT_Int16
    )

    # set transform and projection
    out_ds.SetGeoTransform(dst_gt)
    out_ds.SetProjection(dst_proj.ExportToWkt())

    out_band = out_ds.GetRasterBand(1)
    out_band.WriteArray(band_array)
    out_band.SetNoDataValue(-9999)

    # ***
    # reproject is converting all nodata to zero
    # https://gis.stackexchange.com/questions/158503/9999-no-data-value-becomes-0-when-writing-array-to-gdal-memory-file
    # (issue may have been resolve in gdal 2.0, currently
    # have older version on sciclone)
    #
    # since out data isn't actually changing shape due to the reproj
    # from epsg 4008, just another geographic datum proj
    # we don't really need to reproject, just reassign the proj
    # and fill in the data. hacky and not ideal, but we rarely use
    # python gdal bindings anymore and i don't want to dig into
    # an issue that was probably fixed in a newer version.
    #
    # will look into updating gdal at some point on sciclone,
    # or readdress when/if we need python gdal bindings in future
    # ***
    #
    # # reproject
    # # need to test different resampling methods
    # # (nearest is default, probably used by gdal_translate)
    # # do not actually  think it matters for this case though
    # # as there does not seem to be much if any need for r
    # # resampling when reprojecting between these projections
    # gdal.ReprojectImage(band_ds, out_ds,
    #                     src_proj.ExportToWkt(), dst_proj.ExportToWkt(),
    #                     gdal.GRA_Bilinear)
    #                     # gdal.GRA_NearestNeighbour)

    # close out datasets
    hdf_ds = None
    band_ds = None
    out_ds = None


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
            print "Could not include file in aggregation ({0})".format(file_path)
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


def write_raster(path, data, meta):
    make_dir(os.path.dirname(path))
    meta['dtype'] = data.dtype
    with rasterio.open(path, 'w', **meta) as result:
        try:
            result.write(data)
        except:
            print path
            print meta
            print data.shape
            raise


def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def run(tasks, func, mode="auto"):
    parallel = False
    if mode in ["auto", "parallel"]:
        try:
            from mpi4py import MPI
            parallel = True
        except:
            parallel = False
    elif mode != "serial":
        raise Exception("Invalid `mode` value for script.")
    if parallel:
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()
    else:
        size = 1
        rank = 0
    c = rank
    while c < len(tasks):
        try:
            func(tasks[c])
        except Exception as e:
            print "Error processing: {0}".format(tasks[c])
            # raise
            print e
        c += size
    if parallel:
        comm.Barrier()


# -----------------------------------------------------------------------------


# build day dataframe
day_df = build_data_list(src_base, dst_base, filter_options)


# build month dataframe
month_df = day_df[["path", "year", "year_month"]].groupby("year_month", as_index=False).aggregate({
    "path": [lambda x: tuple(x), "count"],
    "year": "last"
})
month_df.columns = ["year_month", "day_path_list", "count", "year"]

minimum_days_in_month = 20
month_df = month_df.loc[month_df["count"] >= minimum_days_in_month]

month_df["path"] = month_df.apply(
    lambda x: os.path.join(dst_base, "monthly/avhrr_ndvi_v5_{}.tif".format(x["year_month"])), axis=1
)


# build year dataframe
year_df = month_df[["path", "year"]].groupby("year", as_index=False).aggregate({
    "path": [lambda x: tuple(x), "count"]
})
year_df.columns = ["year", "month_path_list", "count"]

year_df["path"] = year_df["year"].apply(
    lambda x: os.path.join(dst_base, "yearly/avhrr_ndvi_v5_{}.tif".format(x))
)


day_qlist = []
for _, row in day_df.iteritems():
    day_qlist.append([row["path"], row["output_path"]])

month_qlist = []
for _, row in month_df.iteritems():
    month_qlist.append([row["year_month"], row["day_path_list"], row["path"]])

year_qlist = []
for _, row in year_df.iteritems():
    year_qlist.append([row["year"], row["month_path_list"], row["path"]])


if "daily" in build_list:
    make_dir(os.path.join(dst_base, "daily"))
    run(day_qlist, prep_daily_data, mode=mode)

if "monthly" in build_list:
    make_dir(os.path.join(dst_base, "monthly"))
    run(month_qlist, prep_monthly_data, mode=mode)

if "yearly" in build_list:
    make_dir(os.path.join(dst_base, "yearly"))
    run(year_qlist, prep_yearly_data, mode=mode)
