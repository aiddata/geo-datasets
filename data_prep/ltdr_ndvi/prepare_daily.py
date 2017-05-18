"""
for use with NDVI product from LTDR raw dataset

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
import numpy as np
from osgeo import gdal, osr

from datetime import datetime
import rasterio


# mode = "serial"
mode = "parallel"

# example commands for parallel job
# qsub -I -l nodes=2:c18c:ppn=16 -l walltime=48:00:00
# mpirun --mca mpi_warn_on_fork 0 --map-by node -np 32 python-mpi /sciclone/home00/sgoodman/active/master/asdf-datasets/data_prep/ltdr_ndvi/prepare_daily.py

if mode == "parallel":
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()



build_list = [
    "daily",
    "monthly",
    "yearly"
]


src_base = "/sciclone/aiddata10/REU/geo/raw/ltdr/ltdr.nascom.nasa.gov/allData/Ver4"

dst_base = "/sciclone/aiddata10/REU/data/rasters/external/global/ltdr/avhrr_ndvi_v4"




# filter options to accept/deny based on sensor, year
# all values must be strings
# do not enable/use both accept/deny for a given field

filter_options = {
    'use_sensor_accept': False,
    'sensor_accept': [],
    'use_sensor_deny': False,
    'sensor_deny': [],
    'use_year_accept': True,
    'year_accept': ['1982', '1983'],
    'use_year_deny': False,
    'year_deny': ['2017']
}


# -----------------------------------------------------------------------------


def build_data_list(input_base, ops):

    # reference object used to eliminate duplicate year / day combos
    # when overlaps between sensors exists, always use data from newer sensor

    if ops['use_sensor_accept'] and ops['use_sensor_deny']:
        raise Exception('Cannot use accept and deny options for sensors')

    if ops['use_year_accept'] and ops['use_year_deny']:
        raise Exception('Cannot use accept and deny options for years')


    ref = OrderedDict()

    # get sensors
    sensors = [
        name for name in os.listdir(input_base)
        if os.path.isdir(os.path.join(input_base, name))
            and name.startswith("N")
            and len(name) == 3
    ]

    if ops['use_sensor_accept']:
        sensors = [i for i in sensors if i in ops['sensor_accept']]
    elif ops['use_sensor_deny']:
        sensors = [i for i in sensors if i not in ops['sensor_deny']]

    sensors.sort()


    for sensor in sensors:

        # get years for sensors
        path_sensor = input_base +"/"+ sensor

        years = [
            name for name in os.listdir(path_sensor)
            if os.path.isdir(os.path.join(path_sensor, name))
        ]

        if ops['use_year_accept']:
            years = [i for i in years if i in ops['year_accept']]
        elif ops['use_year_deny']:
            years = [i for i in years if i not in ops['year_deny']]

        years.sort()

        for year in years:

            if not year in ref:
                ref[year] = {}

            # get days for year
            path_year = path_sensor +"/"+ year
            filenames = [
                name for name in os.listdir(path_year)
                if not os.path.isdir(os.path.join(path_year, name))
                    and name.endswith(".hdf")
                    and name.split(".")[0] == "AVH13C1"
            ]
            filenames.sort()

            for filename in filenames:

                filename = filename[:-4]
                day = filename.split(".")[1][5:]

                # sensor list is sorted so duplicate day will always be newer
                ref[year][day] = filename


            # sort filenames after year finishes
            ref[year] = OrderedDict(
                sorted(ref[year].iteritems(), key=lambda (k,v): v))

    return ref


def prep_daily_data(task, input_base, output_base):

    year, day, filename = task

    sensor = filename.split('.')[2]

    src_file = os.path.join(input_base, sensor, year, filename + ".hdf")

    dst_dir = os.path.join(output_base, 'daily', year)

    try:
        os.makedirs(dst_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    print "{0} {1} {2}".format(sensor, year, day)
    process_daily_data(src_file, dst_dir)


def process_daily_data(input_path, output_dir):
    """Process input raster and create output in output directory

    Unpack NDVI subdataset from a HDF container
    Reproject to EPSG:4326
    Set values <0 to nodata
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
    output_path = "{0}/{1}.tif".format(
        output_dir,
        os.path.basename(os.path.splitext(input_path)[0])
    )

    # open the dataset and subdataset
    hdf_ds = gdal.Open(input_path, gdal.GA_ReadOnly)

    subdataset = 0
    band_ds = gdal.Open(
        hdf_ds.GetSubDatasets()[subdataset][0], gdal.GA_ReadOnly)

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

    # reproject
    # need to test different resampling methods
    # (nearest is default, probably used by gdal_translate)
    # do not actually  think it matters for this case though
    # as there does not seem to be much if any need for r
    # resampling when reprojecting between these projections
    gdal.ReprojectImage(band_ds, out_ds,
                        src_proj.ExportToWkt(), dst_proj.ExportToWkt(),
                        gdal.GRA_Bilinear)

    # clean data
    band_array = out_ds.ReadAsArray().astype(np.int16)
    band_array[np.where(band_array < 0)] = -9999

    out_ds.GetRasterBand(1).WriteArray(band_array)
    out_ds.GetRasterBand(1).SetNoDataValue(-9999)

    # out_ds.GetRasterBand(1).ReadAsArray()

    # close out datasets
    hdf_ds = None
    band_ds = None
    out_ds = None


def prep_monthly_data(task, output_base):
    year, month, month_files = task

    data, meta = aggregate_rasters(file_list=month_files, method="max")
    month_path = os.path.join(output_base, 'monthly', year, "avhrr_ndvi_{0}_{1}.tif".format(year, month))
    write_raster(month_path, data, meta)


def prep_yearly_data(task, output_base):
    year, year_files = task

    data, meta = aggregate_rasters(file_list=year_files, method="mean")
    year_path = os.path.join(output_base, 'yearly', "avhrr_ndvi_{0}.tif".format(year))
    write_raster(year_path, data, meta)


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
    active = None

    for ix, file_path in enumerate(file_list):

        raster = rasterio.open(file_path)
        active = raster.read()

        if store is None:
            store = active.copy()

        else:
            # make sure dimensions match
            if active.shape != store.shape:
                raise Exception("Dimensions of rasters do not match")

            if method == "max":
                store = np.maximum.reduce([store, active])
                # alternate method
                # store = np.vstack([store, active]).max(axis=0)
            elif method == "min":
                store = np.minimum.reduce([store, active])
            elif method == "mean":
                # probably need to make sure it is float type
                store = np.mean([store, active], axis=0)
            elif method == "sum":
                store = np.sum([store, active], axis=0)
            else:
                raise Exception("Invalid method")


    return store, raster.profile


def write_raster(path, data, meta):
    try:
        os.makedirs(os.path.dirname(path))
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    result = rasterio.open(path, 'w', **meta)
    result.write(data)


# -----------------------------------------------------------------------------

print "generating initial data list..."

ref = build_data_list(src_base, filter_options)

# -------------------------------------

print "building day list..."

# day_qlist item format = [year, day, filename]
day_qlist = []
for year in ref:
    for day, filename in ref[year].iteritems():
        day_qlist.append([year, day, filename])

# -------------------------------------

print "running daily data..."

if "daily" in build_list:

    if mode == "parallel":

        c = rank
        while c < len(day_qlist):

            prep_daily_data(day_qlist[c], src_base, dst_base)
            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(day_qlist)):
            prep_daily_data(day_qlist[c], src_base, dst_base)

    else:
        raise Exception("Invalid `mode` value for script.")

# -------------------------------------

print "building month list..."

month_qlist = []

month = None
month_files = []
for year in ref:

    for day, filename in ref[year].iteritems():

        day_path = os.path.join(dst_base, 'daily', year, filename + ".tif")

        cur_month = "{0:02d}".format(
            datetime.strptime("{0}+{1}".format(year, day), "%Y+%j").month)

        # print "{0} {1} {2}".format(year, day, cur_month)
        # print filename

        if month is None:
            month = cur_month

        elif cur_month != month:

            # filenames are sorted, so when month does not match
            # it mean you hit end of month

            list_year = str(int(year) - 1) if month == '12' else year
            month_qlist.append((list_year, month, month_files))

            # print "{0} {1}".format(list_year, month)
            # for i in month_files: print i

            # init next month
            month = cur_month
            month_files = []


        # add day to month list
        month_files.append(day_path)


# make sure to add final month of final year
month_qlist.append((year, month, month_files))

for i in month_qlist: print i

# filter out months with insufficient data
minimum_days_in_month = 20
month_qlist = [i for i in month_qlist if len(i[2]) > minimum_days_in_month]

# -------------------------------------

print "running monthly data..."

if "monthly" in build_list:

    if mode == "parallel":

        c = rank
        while c < len(month_qlist):

            prep_monthly_data(month_qlist[c], dst_base)
            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(month_qlist)):
            prep_monthly_data(month_qlist[c], dst_base)

    else:
        raise Exception("Invalid `mode` value for script.")

# -------------------------------------

print "building year list..."

year_months = {}
for year, month, _ in month_qlist:

    # first year of data, not enough months
    if year == '1981':
        pass

    if not year in year_months:
        year_months[year] = []

    month_path = os.path.join(dst_base, 'monthly', year, "avhrr_ndvi_{0}_{1}.tif".format(year, month))

    year_months[year].append(month_path)


year_qlist = [(year, month_path) for year, month_path in year_months.iteritems()]


# -------------------------------------

print "running yearly data..."

if "yearly" in build_list:

    if mode == "parallel":

        c = rank
        while c < len(year_qlist):

            prep_yearly_data(year_qlist[c], dst_base)
            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(year_qlist)):
            prep_yearly_data(year_qlist[c], dst_base)

    else:
        raise Exception("Invalid `mode` value for script.")

