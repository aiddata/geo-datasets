
import os
import glob
import pandas as pd
import fiona


wrs2_path = "/sciclone/aiddata10/REU/projects/afghanistan_gie/data_prep/afg_canals_wrs2_descending.shp"
wrs2 = fiona.open(wrs2_path)

active_path_row_tuples = [(i['properties']['PATH'], i['properties']['ROW'])
                          for i in wrs2]

active_path_row = ["{0:3d}{1:3d}".format(i[0], i[1]) for i in active_path_row_tuples]




# actual
raw_data = "/sciclone/aiddata10/REU/projects/afghanistan_gie/raw_landsat"
file_list = glob.glob(raw_data+"/*.tar.gz")


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


total_scene_count = data_df.groupby(['path_row'], as_index=False).count()
year_scene_count = data_df.groupby(['path_row', 'year'], as_index=False).count()




