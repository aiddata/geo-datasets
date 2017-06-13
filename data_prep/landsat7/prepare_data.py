
import os
import glob
import pandas as pd
import fiona


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

wrs2_path = "/sciclone/aiddata10/REU/projects/afghanistan_gie/data_prep/afg_canals_wrs2_descending.shp"
wrs2 = fiona.open(wrs2_path)

active_path_row = [str(i['properties']['PR']) for i in wrs2]


# -----------------------------------------------------------------------------
# prepare data info

# actual
compressed_data = "/sciclone/aiddata10/REU/projects/afghanistan_gie/compressed_landsat"
file_list = glob.glob(compressed_data+"/*.tar.gz")


# test
# file_df = pd.read_csv("/home/userw/git/asdf-datasets/data_prep/landsat7/test_scene_list.txt", header=None)
# file_list = list(file_df[0])


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


import tarfile

compressed_data = "/sciclone/aiddata10/REU/projects/afghanistan_gie/compressed_landsat"
uncompressed_data = "/sciclone/aiddata10/REU/projects/afghanistan_gie/uncompressed_landsat"

tar_list = list(enumerate(data_df['file']))


def unpack_scene(data, overwrite=False):
    index, scene_targz = data
    print index
    scene_name = os.path.basename(scene_targz).split('.')[0]
    uncompressed_dir = os.path.join(uncompressed_data, scene_name)
    tar = tarfile.open(scene_targz, 'r:gz')
    # extract just ndvi tif
    ndvi_name = [i for i in tar.getnames() if i.endswith('sr_ndvi.tif')][0]
    if not os.path.isfile(os.path.join(uncompressed_dir, ndvi_name)) or overwrite:
        tar.extract(ndvi_name, path=uncompressed_dir)
    # used to extract everything
    # tar.extractall(uncompressed_dir)


# mode = "serial"
mode = "parallel"

# NOTE: use `qsub jobscript` for running parallel
if mode == "parallel":
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()


if mode == "parallel":

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



raise


# -----------------------------------------------------------------------------
# get files by year-season

process_df = data_df.groupby(['path_row', 'year', 'season'], as_index=False).aggregate(lambda x: tuple(x))

process_df.drop(['path', 'row', 'count'],inplace=True,axis=1)


for index, data in process_df.iterrows():
    print index
    # aggregate files for year-season
    #
import rasterio
r = rasterio.open(ndvi)



tar.getnames()
[i for i in tar.getnames() if i.endswith('sr_ndvi.tif')]
ndvi = tar.extractfile([i for i in tar.getnames() if i.endswith('sr_ndvi.tif')][0])
ndvi is not None












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

