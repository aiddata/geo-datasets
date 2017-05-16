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


def process_data(input_path, output_dir):
    """
    unpack a single subdataset from a HDF container, reprojectm and write to GeoTiff

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

    print input_path
    print output_path

    # open the dataset and subdataset
    hdf_ds = gdal.Open(input_path, gdal.GA_ReadOnly)

    ndvi_subdataset = 0
    band_ds = gdal.Open(hdf_ds.GetSubDatasets()[ndvi_subdataset][0], gdal.GA_ReadOnly)


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

    (lrx, lry, lrz ) = tx.TransformPoint(src_gt[0] + src_gt[1]*band_ds.RasterXSize,
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


# -----------------------------------------------------------------------------



src_base = "/sciclone/aiddata10/REU/geo/raw/ltdr/ltdr.nascom.nasa.gov/allData/Ver4"

dst_base = "/sciclone/aiddata10/REU/data/ltdr/ndvi/daily"


# build list of all [year, day] combos

###

# use accept/deny to manually limit which
# sensors or years will run
# all values must be strings

sensor_accept = []
sensor_deny= []

year_accept = ['2009']
year_deny= []

###

# get sensors
sensors = [
    name for name in os.listdir(src_base)
    if os.path.isdir(os.path.join(src_base, name))
        and name.startswith("N")
        and len(name) == 3
        # and name in sensor_accept
        # and name not in sensor_deny
]
sensors.sort()


# reference object used to eliminate duplicate year / day combos
# when overlaps between sensors exists, always use data from newer sensor
ref = OrderedDict()

for sensor in sensors:

    # get years for sensors
    path_sensor = src_base +"/"+ sensor
    years = [
        name for name in os.listdir(path_sensor)
        if os.path.isdir(os.path.join(path_sensor, name))
            and name in year_accept
            # and name not in year_deny
    ]
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
        ref[year] = OrderedDict(sorted(ref[year].iteritems(), key=lambda (k,v): v))


# qlist item format = [year, day, filename]
qlist = []
for year in ref:
    for day, filename in ref[year].iteritems():
        qlist.append([year, day, filename])



# -----------------------------------------------------------------------------


def prep_data(task, input_base, output_base):

    year, day, filename = item

    sensor = filename.split['.'][2]

    src_file = os.path.join(src_base, sensor, year, filename + ".hdf")

    dst_dir = os.path.join(dst_base, year)

    try:
        os.makedirs(dst_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    print "{0} {1} {2}".format(sensor, year, day)
    process_data(src_file, dst_dir)


# -----------------------------------------------------------------------------


# mode = "parallel"
# # mode = "serial"

# if mode == "parallel":

#     from mpi4py import MPI

#     comm = MPI.COMM_WORLD
#     size = comm.Get_size()
#     rank = comm.Get_rank()

#     c = rank
#     while c < len(qlist):

#         prep_data(qlist[c], src_base, dst_base)
#         c += size

# elif mode == "serial":

#     for c in range(len(qlist)):
#         prep_data(qlist[c], src_base, dst_base)


# else:
#     raise Exception("Invalid `mode` value for script.")



# -----------------------------------------------------------------------------


from datetime import datetime
import rasterio
import numpy as np

master_list = []

month = None
month_files = []

for year in ref:

    for day, filename in ref[year].iteritems():

        day_path = os.path.join(dst_base, year, filename + ".tif")

        cur_month = "{0:02d}".format(
            datetime.strptime("{0}+{1}".format(year, day), "%Y+%j").month)

        print "{0} {1} {2}".format(year, day, cur_month)
        print filename

        if month is None:
            month = cur_month

        elif cur_month != month:

            # filenames are sorted, so when month does not match
            # it mean you hit end of month: so run aggregation
            # aggregate_rasters(file_list=month_files, method="max")
            master_list.append((year, month, len(month_files)))

            # print "{0} {1} {2}".format(sensor, year, month)
            # for i in month_files: print i

            # init next month
            month = cur_month
            month_files = [day_path]


        # add day to month list
        month_files.append(day_path)


"""
# make sure to process final month of final year
aggregate_rasters(file_list=month_files, method="max")
master_list.append((year, month, len(month_files)))
"""


# for i in master_list: print i


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
            store = active

        else:
            # make sure dimensions match
            if active.shape != store.shape:
                raise Exception("Dimensions of rasters do not match")

            if method == "max":
                store = max([store, active])

            elif method == "min":
                pass
            elif method == "mean":
                pass
            elif method == "sum":
                pass
            else:
                raise Exception("Invalid method")


    # use last raster instance as template for result raster instance
    result_path = ""
    result = rasterio.open(result_path, 'w', **raster.profile)
    result.write(store, 1)
    return result




