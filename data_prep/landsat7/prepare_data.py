
import os
import pandas as pd

file_df = pd.read_csv("test_scene_list.txt", header=None)
file_list = list(x[0])

data = []
path_row_combos = []
for f in file_list:
    scene = os.path.basename(f)
    path = scene[4:7]
    row = scene[7:10]
    year = scene[10:14]
    month = scene[14:16]
    day = scene[16:18]
    info = (path, row, year, month, day)
    data.append((info, f))
    print info
    path_row_combos.append((path,row))



