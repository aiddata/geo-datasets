

import os
import errno
import glob
import pandas as pd
import fiona
import rasterio
import numpy as np
from affine import Affine
from rasterio.merge import merge as mosaic



def make_dir(path):
    """Make directory.

    Args:
        path (str): absolute path for directory

    Raise error if error other than directory exists occurs.
    """
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


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


def adjust_pixel_coordinate(val, res):
    """adjust pixel coordinates to uniform grid

    this function uses arbitrary grid based at (0,0)
    specifically for afghanistan (lon, lat both positive)

    needed for landsat (30m or 0.0002695) resolution because 1 degree
    cannot be cleanly divided by the pixel resolution
    """
    mod = val % res
    if mod < res * 0.5:
        new = val - mod
    else:
        new = val + res - mod
    return new


def aggregate_rasters(file_list, method="mean", custom_fun=None):
    """Aggregate multiple rasters

    Aggregates multiple rasters with same features (dimensions, transform,
    pixel size, etc.) and creates single layer using aggregation method
    specified.

    Supported methods: mean (default), max, min, sum

    Arguments
        file_list (list): list of file paths for rasters to be aggregated
        method (str): method used for aggregation
        custom_fun (function): applied to all rasters when they are read in,
                               prior to aggregation

    Return
        result: rasterio Raster instance
    """
    meta_list = []
    for ix, file_path in enumerate(file_list):
        try:
            raster = rasterio.open(file_path)
            meta_list.append(raster.meta)
        except:
            print "Could not include file in aggregation ({0})".format(file_path)

    resolution_list = [i['affine'][0] for i in meta_list]
    if len(set(resolution_list)) != 1:
        for i in meta_list: print i
        print set(resolution_list)
        raise Exception('Resolution of files are different')

    res = resolution_list[0]

    xmin_list = []
    ymax_list = []
    xmax_list = []
    ymin_list = []
    for meta in meta_list:
        tmp_xmin = adjust_pixel_coordinate(meta['affine'][2], res)
        tmp_ymax = adjust_pixel_coordinate(meta['affine'][5], res)
        xmin_list.append(tmp_xmin)
        ymax_list.append(tmp_ymax)
        xmax_list.append(tmp_xmin + meta['width'] * res)
        ymin_list.append(tmp_ymax - meta['height'] * res)

    xmin = min(xmin_list)
    ymax = max(ymax_list)
    xmax = max(xmax_list)
    ymin = min(ymin_list)
    # print (xmin, ymax), (xmax, ymin)

    for ix, file_path in enumerate(file_list):
        raster = rasterio.open(file_path)
        meta = raster.meta
        tmp_xmin = adjust_pixel_coordinate(meta['affine'][2], res)
        tmp_ymax = adjust_pixel_coordinate(meta['affine'][5], res)
        # print meta
        # print tmp_xmin, xmin

        col_start = (xmin - tmp_xmin) / res
        col_stop_diff = abs(((tmp_xmin + meta['width'] * res) - xmax) / res)
        col_stop =  meta['width'] + col_stop_diff

        row_start = (tmp_ymax - ymax) / res
        row_stop_diff = abs(((tmp_ymax - meta['height'] * res) - ymin) / res)
        row_stop = meta['height'] + row_stop_diff

        window = ((int(round(row_start)), int(round(row_stop))),
                  (int(round(col_start)), int(round(col_stop))))

        # print row_stop_diff, col_stop_diff
        # print window

        active = raster.read(masked=True, window=window, boundless=True)

        if custom_fun is not None:
            active = custom_fun(active)

        # print active.shape

        if ix == 0:
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
            #     if ix == 1:
            #         weights = (~store.mask).astype(int)
            #     store = np.ma.average(np.ma.array((store, active)), axis=0,
            #                           weights=[weights, (~active.mask).astype(int)])
            #     weights += (~active.mask).astype(int)

                store = np.ma.array((store*ix, active)).sum(axis=0) / (ix+1)

            elif method == "min":
                store = np.ma.array((store, active)).min(axis=0)
            elif method == "sum":
                store = np.ma.array((store, active)).sum(axis=0)
            elif method == "var":
                store = np.ma.array((store, active)).var(axis=0)
            elif method == "sd":
                store = np.ma.array((store, active)).std(axis=0)

            else:
                raise Exception("Invalid method")

    output_profile = raster.profile.copy()
    output_profile['transform'] = Affine(res, 0, xmin, 0, -res, ymax)
    output_profile['width'] = int(round((xmax - xmin) / res))
    output_profile['height'] = int(round((ymax - ymin) / res))

    store = store.filled(raster.nodata)
    return store, output_profile


def create_mosaic(tile_list):

    mosaic_scenes = [rasterio.open(path) for path in tile_list]
    mosaic_profile = mosaic_scenes[0].profile

    mosaic_array, transform = mosaic(mosaic_scenes)

    for i in mosaic_scenes: i.close()

    if 'affine' in mosaic_profile:
        mosaic_profile.pop('affine')

    mosaic_profile["transform"] = transform
    mosaic_profile['height'] = mosaic_array.shape[1]
    mosaic_profile['width'] = mosaic_array.shape[2]
    mosaic_profile['driver'] = 'GTiff'

    return mosaic_array, mosaic_profile


# -----------------------------------------------------------------------------


run_agg = True
run_mosaic = True


mode = "parallel"
# mode = "serial"


monthly_tiles = "/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/filtered_monthly"

yearly_tiles = "/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/filtered_yearly"

yearly_mosaics = "/sciclone/aiddata10/REU/geo/data/rasters/external/global/viirs/vcmcfg_dnb_composites_v10/yearly"

tile_id_list = ["00N060E", "00N060W", "00N180W", "75N060E", "75N060W", "75N180W"]

aggregation_methods = ["max", "mean", "var", "std"]
aggregation_methods = ["mean"]
aggregation_methods = ["max", "var", "std"]


# -----------------------------------------------------------------------------


year_month_list = os.listdir(monthly_tiles)

year_month_dict = {}
for i in year_month_list:
    year, month = i[:4], i[4:]
    if year not in year_month_dict:
        year_month_dict[year] = []
    year_month_dict[year].append(month)

for year in year_month_dict.keys():
    month_list = year_month_dict[year]
    if '12' not in month_list:
        print "Removing year: {0} with insufficient months ({1})".format(year, month_list)
        del year_month_dict[year]


# -----------------------------------------------------------------------------


tile_qlist = [(year, tile_id) for year in year_month_dict.keys() for tile_id in tile_id_list]
tile_qlist.sort()

def run_yearly_tile_agg(year, tile_id):
    tile_files =  glob.glob(monthly_tiles + "/{0}*/*{1}*.avg_rade9.tif".format(year, tile_id))
    year_dir = os.path.join(yearly_tiles, year)
    for method in aggregation_methods:
        print "Running {0} {1} {2}".format(year, tile_id, method)
        array = None
        array, profile = aggregate_rasters(tile_files, method=method)
        file_name = "{0}_{1}_{2}.tif".format(year, tile_id, method)
        path = os.path.join(year_dir, file_name)
        write_raster(path, array, profile)
        print "\tFinished {0} {1} {2}".format(year, tile_id, method)


if run_agg:

    if mode == "parallel":
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        c = rank
        while c < len(tile_qlist):
            try:
                run_yearly_tile_agg(*tile_qlist[c])
            except Exception as e:
                print "Error processing tiles: {0}".format(tile_qlist[c])
                raise

            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(tile_qlist)):
            try:
                run_yearly_tile_agg(*tile_qlist[c])
            except Exception as e:
                print "Error processing tiles: {0}".format(tile_qlist[c])
                raise

    else:
        raise Exception("Invalid `mode` value for script.")


# -----------------------------------------------------------------------------


agg_qlist = [(year, method) for year in year_month_dict.keys() for method in aggregation_methods]
agg_qlist.sort()

def run_yearly_tile_mosaic(year, method):
    print "Running {0} {1}".format(year, method)

    tile_list = glob.glob(yearly_tiles + "/{0}/{0}_*_{1}.tif".format(year, method))

    if len(tile_list) != 6:
        raise Exception("Bad tile count ({0} {1}: {2})".format(year, method, len(tile_list)))

    array, profile = create_mosaic(tile_list)

    mosaic_output_path = os.path.join(yearly_mosaics, method, "{0}_{1}.tif".format(year, method))
    write_raster(mosaic_output_path, array, profile)

    print "\tFinished {0} {1}".format(year, method)


if run_mosaic:

    if mode == "parallel":
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        c = rank
        while c < len(agg_qlist):
            try:
                run_yearly_tile_mosaic(*agg_qlist[c])
            except Exception as e:
                print "Error processing mosaic: {0}".format(agg_qlist[c])
                raise

            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(agg_qlist)):
            try:
                run_yearly_tile_mosaic(*agg_qlist[c])
            except Exception as e:
                print "Error processing mosaic: {0}".format(agg_qlist[c])
                raise

    else:
        raise Exception("Invalid `mode` value for script.")


