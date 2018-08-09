

import os
import sys
import errno
import rasterio
import numpy as np


# mode = "serial"
mode = "parallel"

# NOTE: use `qsub jobscript` for running parallel
if mode == "parallel":
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()


src_base = "/sciclone/aiddata10/REU/geo/data/rasters/gpm/monthly"
dst_base = "/sciclone/aiddata10/REU/geo/data/rasters/gpm/yearly"

year_mask = "gpm_precipitation_YYYY.tif"
year_sep = "_"
year_loc = 2


# -------------------------------------


print "building year list..."

year_months = {}

month_files = [i for i in os.listdir(src_base) if i.endswith('.tif')]

for mfile in month_files:

    year = mfile.split(year_sep)[year_loc]

    if year not in year_months:
        year_months[year] = list()

    year_months[year].append(os.path.join(src_base, mfile))


year_qlist = [
    (year, month_paths) for year, month_paths in year_months.iteritems()
    if len(month_paths) == 12
]


# -------------------------------------


def write_raster(path, data, meta):
    try:
        os.makedirs(os.path.dirname(path))
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    meta['dtype'] = data.dtype

    with rasterio.open(path, 'w', **meta) as result:
        result.write(data)


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


def run_yearly_data(task):
    year, year_files = task
    data, meta = aggregate_rasters(file_list=year_files, method="mean")
    year_path = os.path.join(dst_base, year_mask.replace("YYYY", str(year)))
    write_raster(year_path, data, meta)


print "running yearly data..."

if mode == "parallel":

    c = rank
    while c < len(year_qlist):

        try:
            run_yearly_data(year_qlist[c])
        except Exception as e:
            print "Error processing year: {0}".format(year_qlist[c][0])
            # raise
            print e
            # raise Exception('year processing')


        c += size

    comm.Barrier()

elif mode == "serial":

    for c in range(len(year_qlist)):
        run_yearly_data(year_qlist[c])

else:
    raise Exception("Invalid `mode` value for script.")
