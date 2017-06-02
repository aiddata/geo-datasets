
import os
import pandas as pd

file_df = pd.read_csv("test_scene_list.txt", header=None)
file_list = list(file_df[0])

data = []
for f in file_list:
    scene = os.path.basename(f)
    path = scene[4:7]
    row = scene[7:10]
    year = scene[10:14]
    month = scene[14:16]
    day = scene[16:18]
    info = (path, row, year, month, day, f)
    data.append(info)


columns = ("path", "row", "year", "month", "day", "file")

data_df = pd.DataFrame(data, columns=columns)







