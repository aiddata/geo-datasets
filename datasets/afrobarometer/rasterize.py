
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from affine import Affine
from distancerasters import rasterize

# -*- coding: utf-8 -*-

file = r"/Users/miranda/Documents/AidData/datasets/AFB/rasterization/Merged Stacked Datasets_v3.dta"
# file = r"/sciclone/home10/zlv/datasets/data_process/afb/afb_data_merged.csv"

# ------------------------------------
# create output directory

dirc = os.path.dirname(file)

for rd in range(1, 7):
    folder = "round_" + str(rd)
    fpath = os.path.join(dirc, folder)
    if not os.path.isdir(fpath):
        os.mkdir(fpath)

# ----------------------

def list2matrix(lst, n):
    """
    :param lst: a list of values
    :param n: output matrix length
    :return: a matrix of n*n with list values
    """

    mx = (n,n)
    mx = np.ones(mx) * 255

    rows = len(lst)/n
    last_col = len(lst) % n

    for row in range(0,rows):
        start = row*n
        end = (row + 1)*n
        mx[row,:] = lst[start:end]

    mx[rows,0:last_col] = lst[(len(lst)-last_col):]

    return mx

# ------------------------------------------
dta = pd.read_stata(file)

# Set filters: each questions per round
questions = ["trust_pres"]#, "trust_police", "trust_court", "trust_electcom",
             #"trust_party", "trust_oppart"]

# ------ rasterization setting ---------

n = 5
initial_pixel = 0.025
pixel_size = initial_pixel/n

out_shape = (int(180 / pixel_size), int(360 / pixel_size))

affine = Affine(pixel_size, 0, -180,
                0, -pixel_size, 90)

nan_val = 255

# --------------------------------------------
# define precision categories

is_precise = ((dta["location_class"] == 2) | (dta["location_class"] == 3) | (
(dta["location_class"] == 1) & (dta["location_type_code"].isin(["ADM3", "ADM4", "ADM4H", "ADM5"]))))
dta['category'] = None

dta.loc[is_precise, 'category'] = 'fine'
dta.loc[~is_precise, 'category'] = 'coarse'

# -------------------------------------

for rd in range(6, 7, 1):

    print "Start working on Round: ", str(rd)

    # set path
    folder = "round_" + str(rd)
    fpath = os.path.join(dirc, folder)

    dta_gdf = dta.loc[dta['round'] == rd]
    rasterdf = dta_gdf.loc[dta_gdf["category"] == "fine"].copy()

    for question in questions:

        if not rasterdf[question].isnull().all():
            print "Working on Round " + str(rd) + " Question " + question
            print "---------------------------------------------------"

        # ----------------------------------Start rasterize----------------------------------

            # assign null value
            rasterdf.loc[rasterdf[question].isnull(),question] = nan_val

            # get response list
            responses = sorted(list(rasterdf[question].unique()))

            # round coordinates
            rasterdf['latitude'] = rasterdf['latitude'].apply(lambda x: round(x*20)/20)
            rasterdf['longitude'] = rasterdf['longitude'].apply(lambda x: round(x*20)/20)

            rastergp = rasterdf.groupby(['longitude', 'latitude'])

            outdict = dict()
            outdict['longitude'] = list()
            outdict['latitude'] = list()
            outdict[question] = list()

            for name, group in rastergp:

                responselist = list()

                for response in responses:

                    if not response== nan_val:

                        respcount = group[group[question]==response][question].count()
                        responselist.append(str(int(response)+100)+str(respcount))

                # create n*n matrix and assign values
                minimatrix = list2matrix(responselist, n)

                mx = (n, n)
                mx = np.ones(mx) * 255

                # initial point is at the top left cornor
                # longitude moves n/2 pixels left (2 pixels)
                # latitude moves n/2 pixels above (2 pixels)

                lon_init = name[0] - pixel_size * (n/2)
                lat_init = name[1] + pixel_size * (n/2)

                count = 0

                # assign coordinates to the matrix that holds response values (minimatrix)
                for i in range(0, n, 1):

                    for j in range(0, n, 1):

                        if count >= len(responselist):
                            break
                        else:
                            outdict['latitude'].append((lat_init + i * pixel_size))
                            outdict['longitude'].append((lon_init + j * pixel_size))
                            outdict[question].append(minimatrix[i, j])
                            count = count + 1

            newdf = pd.DataFrame.from_dict(outdict)

            newdf["geometry"] = newdf.apply(lambda x: Point(x["longitude"], x["latitude"]), axis=1)
            newgdf = gpd.GeoDataFrame(newdf)

            name = "round_{0}_{1}.tif".format(rd, question)
            output = os.path.join(fpath, name)

            cat_raster, _ = rasterize(newgdf, affine=affine, shape=out_shape, attribute=question, nodata=nan_val, fill=255, output=output)