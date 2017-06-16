
import os
import errno
import glob
import pandas as pd
import fiona


# -----------------------------------------------------------------------------
"""
To run job, use jobscript

For interactive job (debug)
use following lines to start job, then run (parallel)

qsub -I -l nodes=5:c18c:ppn=16 -l walltime=48:00:00
mpirun --mca mpi_warn_on_fork 0 --map-by node -np 80 python-mpi
    /sciclone/home00/sgoodman/active/master/asdf-datasets/data_prep/landsat7/prepare_data.py

"""

# use for testing without access to sciclone filesystem
# (assumes you are running script out of git repo "~/git/asdf-datasets")
test_mode = 0

run_scene_unpack = False
run_season_agg = False
run_mosaic = True

# mode = "serial"
mode = "parallel"


# -----------------------------------------------------------------------------

project_dir = "/sciclone/aiddata10/REU/projects/afghanistan_gie"

# -----------------------------------------------------------------------------
# define seasons

seasons = {
    'winter': [12, 1, 2],
    'spring': [3, 4, 5],
    'summer': [6, 7, 8],
    'fall': [9, 10, 11]
}
seasons = {k: map("{:02d}".format, map(int, v)) for k, v in seasons.iteritems()}


def get_season(month):
    for k, v in seasons.iteritems():
        if month in v:
            return k
    return None


# -----------------------------------------------------------------------------
# define active path rows

if test_mode:
    wrs2_path = os.path.expanduser(
        "~/git/asdf-datasets/data_prep/landsat7/test_data/afg_canals_wrs2_descending.shp")
else:
    wrs2_path = os.path.join(project_dir, "data_prep/afg_canals_wrs2_descending.shp")

wrs2 = fiona.open(wrs2_path)

active_path_row = [str(i['properties']['PR']) for i in wrs2]


# -----------------------------------------------------------------------------
# prepare data info


if test_mode:
    test_scene_list = os.path.expanduser(
        "~/git/asdf-datasets/data_prep/landsat7/test_data/test_scene_list.txt")
    file_df = pd.read_csv(test_scene_list, header=None)
    file_list = list(file_df[0])
else:
    compressed_data = os.path.join(project_dir, "compressed_landsat")
    file_list = glob.glob(compressed_data+"/*.tar.gz")


data = []
for f in file_list:
    scene = os.path.basename(f)
    path = scene[4:7]
    row = scene[7:10]
    path_row = path + row
    if path_row in active_path_row:
        year = scene[10:14]
        month = scene[14:16]
        day = scene[16:18]
        info = (path, row, path_row, year, month, day, f)
        data.append(info)


columns = ("path", "row", "path_row", "year", "month", "day", "file")

data_df = pd.DataFrame(data, columns=columns)

data_df['count'] = 1
data_df['season'] = data_df.apply(lambda z: get_season(z.month), axis=1)


# -----------------------------------------------------------------------------


def unpack_scene(data, overwrite=False):
    index, scene_targz = data
    print index
    scene_name = os.path.basename(scene_targz).split('.')[0]
    uncompressed_data = os.path.join(project_dir, "uncompressed_landsat")
    uncompressed_dir = os.path.join(uncompressed_data, scene_name)
    tar = tarfile.open(scene_targz, 'r:gz')
    # extract just ndvi tif
    ndvi_name = [i for i in tar.getnames() if i.endswith('sr_ndvi.tif')][0]
    if not os.path.isfile(os.path.join(uncompressed_dir, ndvi_name)) or overwrite:
        tar.extract(ndvi_name, path=uncompressed_dir)
    # used to extract everything
    # tar.extractall(uncompressed_dir)


def run_data_unpacking(tar_list, mode):
    if mode == "parallel":
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        c = rank
        while c < len(tar_list):

            try:
                unpack_scene(tar_list[c])
            except Exception as e:
                print "Error processing scene: {0} ({1})".format(*tar_list[c])
                print e
                # raise Exception('day processing')

            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(tar_list)):
            unpack_scene(tar_list[c])

    else:
        raise Exception("Invalid `mode` value for script.")


# -------------------------------------


import tarfile

tar_list = list(enumerate(data_df['file']))

if run_scene_unpack:
    run_data_unpacking(tar_list, mode)



# -----------------------------------------------------------------------------
# get files by year-season

process_df = data_df.groupby(
    ['path_row', 'year', 'season'], as_index=False).aggregate(lambda x: tuple(x))

process_df.drop(['path', 'row', 'count'],inplace=True,axis=1)

def convert_compressed_file_name(x):
    """convert compressed file names to dir

    input tuple of tar.gz files converted to
    output tuple of dir which contains extracted files
    """
    return tuple([
        i[:-len('.tar.gz')].replace('compressed', 'uncompressed') for i in x
    ])


process_df['folder'] = process_df['file'].apply(
    lambda z: convert_compressed_file_name(z))


# -----------------------------------------------------------------------------



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
            # print "hi"
            # print raster.meta
            # print raster.profile
            # print "bye"
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
                if ix == 1:
                    weights = (~store.mask).astype(int)
                store = np.ma.average(np.ma.array((store, active)), axis=0,
                                      weights=[weights, (~active.mask).astype(int)])
                weights += (~active.mask).astype(int)
            elif method == "min":
                store = np.ma.array((store, active)).min(axis=0)
            elif method == "sum":
                store = np.ma.array((store, active)).sum(axis=0)
            else:
                raise Exception("Invalid method")

    output_profile = raster.profile.copy()
    output_profile['transform'] = Affine(res, 0, xmin, 0, -res, ymax)
    output_profile['width'] = int(round((xmax - xmin) / res))
    output_profile['height'] = int(round((ymax - ymin) / res))

    store = store.filled(raster.nodata)
    return store, output_profile



def write_raster(path, data, meta):
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


def raster_calc(raster):
    raster[np.where((raster < 0) & (raster > -9999))] = 0
    raster[np.where(raster == 20000)] = 10000
    return raster



def prepare_season(index, data):
    print "{0}) {1} {2} {3}".format(index, data['year'], data['path_row'], data['season'])

    file_list = [
        filter(
            lambda x: x.endswith('_sr_ndvi.tif'),
            [os.path.join(d, f) for f in os.listdir(d)]
        )
        for d in data['folder']
    ]

    file_list = [y for x in file_list for y in x]


    # aggregate files for year-season
    scene_array, scene_profile = aggregate_rasters(file_list, method="max",
                                                   custom_fun=raster_calc)

    scene_season_data = os.path.join(project_dir, "season_scenes")
    scene_season_name = "{0}_{1}_{2}.tif".format(
        data['year'], data['path_row'], data['season'])
    scene_season_path = os.path.join(scene_season_data, scene_season_name)

    print "writing {0}".format(scene_season_path)
    write_raster(scene_season_path, scene_array, scene_profile)



def run_season_aggregation(process_df, mode):
    if mode == "parallel":
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        c = rank
        while c < len(process_df):

            try:
                prepare_season(c, process_df.iloc[c])
            except Exception as e:
                print "Error processing season-scene: {0}".format(c)
                print e
                # raise Exception('something')

            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(process_df)):
            prepare_season(c, process_df.iloc[c])

    else:
        raise Exception("Invalid `mode` value for script.")


# -------------------------------------


import rasterio
import numpy as np
from affine import Affine


if run_season_agg:
    run_season_aggregation(process_df, mode)


# -----------------------------------------------------------------------------
# mosaic scenes for each season



def build_mosaic(index, data):
    print "{0} {1}".format(data['year'], data['season'])
    if len(data['path_row']) != len(active_path_row):
        print data['path_row']

    season_scene_files = [
        os.path.join(
            project_dir,
            "season_scenes",
            "{0}_{1}_{2}.tif".format(data['year'], pr, data['season']))
        for pr in data['path_row']
    ]

    mosaic_output_path = os.path.join(
        project_dir, "season_mosaics", "{0}_{1}.tif".format(data['year'], data['season']))

    mosaic_scenes = [rasterio.open(path) for path in season_scene_files]
    mosaic_profile = mosaic_scenes[0].profile

    mosaic_array, transform = scene_mosaic(mosaic_scenes)

    for i in mosaic_scenes: i.close()

    if 'affine' in mosaic_profile:
        mosaic_profile.pop('affine')

    mosaic_profile["transform"] = transform
    mosaic_profile['height'] = mosaic_array.shape[1]
    mosaic_profile['width'] = mosaic_array.shape[2]
    mosaic_profile['driver'] = 'GTiff'

    mosaic = rasterio.open(mosaic_output_path, 'w', **mosaic_profile)
    mosaic.write(mosaic_array)
    mosaic.close()


def run_mosaic_builder(mosaic_df, mode):
    if mode == "parallel":
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        c = rank
        while c < len(mosaic_df):

            try:
                build_mosaic(c, mosaic_df.iloc[c])
            except Exception as e:
                print "Error building mosaic: {0}".format(c)
                print e
                # raise Exception('something')

            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(mosaic_df)):
            build_mosaic(c, mosaic_df.iloc[c])

    else:
        raise Exception("Invalid `mode` value for script.")




from rasterio.merge import merge as scene_mosaic


mosaic_df = process_df[['path_row', 'year', 'season']].groupby(
    ['year', 'season'], as_index=False).aggregate(lambda x: tuple(x))


if run_mosaic:
    run_mosaic_builder(mosaic_df, mode)





# -----------------------------------------------------------------------------


assert(0)


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# data checks

# files per scene
total_scene_count = data_df.groupby(['path_row'], as_index=False).count()

# files per scene per year
year_scene_count = data_df.groupby(['path_row', 'year'], as_index=False).count()

# files per scene per month
month_scene_count = data_df.groupby(['path_row', 'year', 'month'], as_index=False).count()

# files per scene per season
season_scene_count = data_df.groupby(['path_row', 'year', 'season'], as_index=False).count()


# -------------------------------------


# create new dataframe where every month has entry
# so we can see where months are missing

start_year = int(data_df['year'].min())
end_year = int(data_df['year'].max())

year_list = range(start_year, end_year+1)
year_col = [y for y in year_list for i in range(12) ]
month_col = range(1, 13) * len(year_list)

template_scene_df = pd.DataFrame({
    'year': map(str, year_col),
    'month': map("{:02d}".format, month_col),
    'count': 0 * len(year_col),
    'path_row': 0
})


def build_scene_df(template, path_row):
    template['path_row'] = path_row
    print path_row
    return template

scene_df_list = [build_scene_df(template_scene_df.copy(), pr) for pr in active_path_row]

full_df = pd.concat(scene_df_list)


def get_scene_count(scene_month_data):
    match = data_df.loc[
        (data_df['year'] == scene_month_data['year'] )
        & (data_df['month'] == scene_month_data['month'])
        & (data_df['path_row'] == scene_month_data['path_row'])
    ]
    return len(match)


full_df['count'] = full_df.apply(lambda z: get_scene_count(z), axis=1)

full_df['season'] = full_df.apply(lambda z: get_season(z.month), axis=1)

# -------------------------------------

# files per scene
alt_total_scene_count = full_df[['path_row', 'count']].groupby(['path_row'], as_index=False).sum()

# files per scene per year
alt_year_scene_count = full_df.groupby(['path_row', 'year'], as_index=False).sum()

# files per scene per month
alt_season_scene_count = full_df.groupby(['path_row', 'year', 'month'], as_index=False).sum()

# files per scene per season
alt_season_scene_count = full_df.groupby(['path_row', 'year', 'season'], as_index=False).sum()

# -------------------------------------


# total missing scene-months
print sum(full_df['count'] == 0)

# number of scene-seasons without any data
print sum(alt_season_scene_count['count'] == 0)

# print scene-seasons without any data
print alt_season_scene_count.loc[(alt_season_scene_count['count'] == 0)]

