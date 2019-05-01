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
import numpy as np
from osgeo import gdal, osr

from datetime import datetime
import rasterio

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

    # reference object used to eliminate duplicate year / day combos
    # when overlaps between sensors exists, always use data from newer sensor

    if ops['use_sensor_accept'] and ops['use_sensor_deny']:
        raise Exception('Cannot use accept and deny options for sensors')

    if ops['use_year_accept'] and ops['use_year_deny']:
        raise Exception('Cannot use accept and deny options for years')


    ref = OrderedDict()

    # get sensors
    sensors = [
        name.split("_")[0] for name in os.listdir(input_base)
        if os.path.isdir(os.path.join(input_base, name))
            and name.startswith("N")
            and len(name.split("_")[0]) == 3
            and name.endswith("_AVH13C1")
    ]

    if ops['use_sensor_accept']:
        sensors = [i for i in sensors if i in ops['sensor_accept']]
    elif ops['use_sensor_deny']:
        sensors = [i for i in sensors if i not in ops['sensor_deny']]

    sensors.sort()


    for sensor in sensors:

        # get years for sensors
        path_sensor = input_base +"/"+ sensor+"_AVH13C1"

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


def prep_daily_data(task):
    year, day, filename, input_base, output_base = task
    sensor = filename.split('.')[2]
    src_file = os.path.join(input_base, sensor, year, filename + ".hdf")
    print "{0} {1} {2}".format(sensor, year, day)
    process_daily_data(src, dst)


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

if rank == 0: print "generating initial data list..."

ref = build_data_list(src_base, filter_options)

# -------------------------------------

if rank == 0: print "building day list..."

# day_qlist item format = [year, day, filename]
day_qlist = []
for year in ref:
    for day, filename in ref[year].iteritems():
        day_qlist.append([year, day, filename, src_base, dst_base])


# -------------------------------------

if rank == 0: print "building month list..."

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
month_qlist.append((year, month, month_files, dst_base))

if rank == 0:
    for i in month_qlist: print "{0} {1} {2}".format(i[0], i[1], len(i[2]))

# filter out months with insufficient data
minimum_days_in_month = 20
month_qlist = [i for i in month_qlist if len(i[2]) > minimum_days_in_month]

# -------------------------------------

if rank == 0: print "building year list..."

year_months = {}
for year, month, _ in month_qlist:

    # first year of data, not enough months
    if year == '1981':
        pass

    if not year in year_months:
        year_months[year] = []

    month_path = os.path.join(dst_base, 'monthly', year, "avhrr_ndvi_{0}_{1}.tif".format(year, month))

    year_months[year].append(month_path)


year_qlist = [(year, month_path, dst_base) for year, month_path in year_months.iteritems()]


# -------------------------------------

if rank == 0: print "running daily data..."

if "daily" in build_list:
    run(day_qlist, prep_daily_data, mode=mode)

if rank == 0: print "running monthly data..."

if "monthly" in build_list:
    run(month_qlist, prep_monthly_data, mode=mode)

if rank == 0: print "running yearly data..."

if "yearly" in build_list:
    run(year_qlist, prep_yearly_data, mode=mode)
