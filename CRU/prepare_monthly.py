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

import rasterio

class DataAggregation(object):

    def __init__(self, src_base=None, dst_base=None, filter_options=None, prefix=None):

        self.dst_base = dst_base
        self.src_base = src_base
        self.filter_options = filter_options
        self.prefix = prefix


# -----------------------------------------------------------------------------


    def build_data_list(self):

        # reference object used to eliminate duplicate year / day combos
        # when overlaps between sensors exists, always use data from newer sensor

        if self.filter_options['use_sensor_accept'] and self.filter_options['use_sensor_deny']:
            raise Exception('Cannot use accept and deny options for sensors')

        if self.filter_options['use_year_accept'] and self.filter_options['use_year_deny']:
            raise Exception('Cannot use accept and deny options for years')


        ref = OrderedDict()

        # get sensors
        sensors = [
            name for name in os.listdir(self.src_base)
            if os.path.isdir(os.path.join(self.src_base, name))
                and name.startswith("N")
                and len(name) == 3
        ]

        if self.filter_options['use_sensor_accept']:
            sensors = [i for i in sensors if i in self.filter_options['sensor_accept']]
        elif self.filter_options['use_sensor_deny']:
            sensors = [i for i in sensors if i not in self.filter_options['sensor_deny']]

        sensors.sort()


        for sensor in sensors:

            # get years for sensors
            path_sensor = self.src_base +"/"+ sensor

            years = [
                name for name in os.listdir(path_sensor)
                if os.path.isdir(os.path.join(path_sensor, name))
            ]

            if self.filter_options['use_year_accept']:
                years = [i for i in years if i in self.filter_options['year_accept']]
            elif self.filter_options['use_year_deny']:
                years = [i for i in years if i not in self.filter_options['year_deny']]

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






    def prep_daily_data(self, task):

        year, day, filename = task

        sensor = filename.split('.')[2]

        src_file = os.path.join(self.src_base, sensor, year, filename + ".hdf")

        dst_dir = os.path.join(self.dst_base, 'daily', year)

        try:
            os.makedirs(dst_dir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        print "{0} {1} {2}".format(sensor, year, day)
        self.process_daily_data(src_file, dst_dir)


    def process_daily_data(self, input_path, output_dir):
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
        output_path = "{0}/{1}.tif".format(
            output_dir,
            os.path.basename(os.path.splitext(input_path)[0])
        )

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



    def prep_monthly_data(self, task):
        year, month, month_files = task

        data, meta = self.aggregate_rasters(file_list=month_files, method="max")
        month_path = os.path.join(self.dst_base, 'monthly', year, "{0}_{1}_{2}.tif".format(self.prefix, year, month))
        self.write_raster(month_path, data, meta)


    def prep_yearly_data(self, task):
        year, year_files = task

        data, meta = self.aggregate_rasters(file_list=year_files, method="mean")
        year_path = os.path.join(self.dst_base, 'yearly',"{0}_{1}.tif".format(self.prefix, year))
        self.write_raster(year_path, data, meta)


    def aggregate_rasters(self, file_list, method="mean"):
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


    def write_raster(self, path, data, meta):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        meta['dtype'] = data.dtype

        with rasterio.open(path, 'w', **meta) as result:
            try:
                result.write(data)
            except:
                print path
                print meta
                print data.shape
                raise
