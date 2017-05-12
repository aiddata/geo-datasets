"""
for use with NDVI product from LTDR raw dataset (AVH13C1)

example LTDR product file names

AVH02C1.A1981299.N07.004.2013228133954.hdf
AVH09C1.A1981211.N07.004.2013228083053.hdf
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

ndvi product code is AVH13C1
"""

import os
import numpy as np
from osgeo import gdal, osr


input_path = "/home/userw/Desktop/AVH13C1.A2010153.N19.004.2015206181729.hdf"
output_dir = "/home/userw/Desktop"


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
        output_dir
        os.path.basename(os.path.splitext(input_path)[0]))


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




path_base = "/sciclone/aiddata10/REU/geo/raw/ltdr/ltdr.nascom.nasa.gov/allData/Ver4"


# reference object used to eliminate duplicate year / day combos
# when overlaps between sensors exists, always use data from newer sensor
ref = {}

# list of all [year, day] combos

sensor_accept = ["N18"]

# get sensors
sensors = [name for name in os.listdir(path_base)
           if os.path.isdir(os.path.join(path_base, name)) and name in sensor_accept]
sensors.sort()

# use limited sensors for testing
# sensors = ['2001']

for sensor in sensors:

    # get years for sensors
    path_sensor = path_base +"/"+ sensor
    years = [name for name in os.listdir(path_sensor)
             if os.path.isdir(os.path.join(path_sensor, name))]
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

            if not day in ref[year] or int(sensor[1:]) > int(ref[year][day][0][1:]):
                # print "\n" + str(year) +" "+ str(day) +" "+ str(sensor) +" "+ str(filename)
                ref[year][day] = [sensor, filename]


# list final [sensor, year, day] combos from reference object
# qlist = [ref[year][day] + [year, day] for day in ref[year] for year in ref if year in ref and day in ref[year]]

qlist = []
for year in ref:
    for day in ref[year]:
        qlist.append(ref[year][day] + [year, day])
        # print qlist

for c in range(len(qlist)):




