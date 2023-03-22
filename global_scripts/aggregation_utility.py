"""
RasterAggregation
for use with raster data

- Prepares list of all files
- Builds list of day files to process
- Processes day files
- Builds list of day files to aggregate to months
- Run month aggregation
- Builds list of month files to aggregate to years
- Run year aggregation

"""

import os
import errno
from collections import OrderedDict
import numpy as np
from osgeo import gdal, osr

import rasterio


class RasterAggregation(object):

    def __init__(self, src_base=None, dst_base=None, filter_options=None, prefix=None):

        self.dst_base = dst_base
        self.src_base = src_base
        self.filter_options = filter_options
        self.prefix = prefix


# -----------------------------------------------------------------------------


    def build_data_list(self):

        # reference object used to eliminate duplicate year / day combos
        # when overlaps between sensors exists, always use data from newer sensor


        if self.filter_options['use_year_accept'] and self.filter_options['use_year_deny']:
            raise Exception('Cannot use accept and deny options for years')


        ref = OrderedDict()

        years = [
            name for name in os.listdir(self.src_base)
            if os.path.isdir(os.path.join(self.src_base, name))
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
            path_year = self.src_base +"/"+ year
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
            result.write(data)

