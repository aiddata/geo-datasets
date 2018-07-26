""" the projection function is edited based on scripts: https://hdfeos.org/zoo/NSIDC/MOD10A1_Snow_Cover_Daily_Tile.py
"""

from pyhdf.SD import SD, SDC
import distancerasters
from affine import Affine
import os
import re

import numpy as np
import pyproj

def mk_proj(inhdf, outdir):

    # get data time stamp
    datestring = inhdf.split(".")[1][1:]
    newtime = get_time(datestring)

    # get new data export name
    dayout = os.path.join(outdir, "modis_lst_day_cmg_" + newtime + ".tif")
    nightout = os.path.join(outdir, "modis_lst_night_cmg_" + newtime + ".tif")


    hdf = SD(inhdf, SDC.READ)

    # Read dataset.
    data2D_day = hdf.select('LST_Day_CMG')
    data_day = data2D_day[:, :].astype(np.float64)

    data2D_night = hdf.select('LST_Night_CMG')
    data_night = data2D_night[:, :].astype(np.float64)

    # Read global attribute.
    fattrs = hdf.attributes(full=1)
    ga = fattrs["StructMetadata.0"]
    gridmeta = ga[0]

    # Construct the grid.  The needed information is in a global attribute
    # called 'StructMetadata.0'.  Use regular expressions to tease out the
    # extents of the grid.

    ul_regex = re.compile(r'''UpperLeftPointMtrs=\(
                                      (?P<upper_left_x>[+-]?\d+\.\d+)
                                      ,
                                      (?P<upper_left_y>[+-]?\d+\.\d+)
                                      \)''', re.VERBOSE)
    match = ul_regex.search(gridmeta)
    x0 = np.float(match.group('upper_left_x'))
    y0 = np.float(match.group('upper_left_y'))

    lr_regex = re.compile(r'''LowerRightMtrs=\(
                                      (?P<lower_right_x>[+-]?\d+\.\d+)
                                      ,
                                      (?P<lower_right_y>[+-]?\d+\.\d+)
                                      \)''', re.VERBOSE)
    match = lr_regex.search(gridmeta)

    x1 = np.float(match.group('lower_right_x'))
    y1 = np.float(match.group('lower_right_y'))
    ny, nx = data_day.shape
    xinc = (x1 - x0) / nx
    yinc = (y1 - y0) / ny


    x = np.linspace(x0, x0 + xinc * nx, nx)
    y = np.linspace(y0, y0 + yinc * ny, ny)
    xv, yv = np.meshgrid(x, y)

    # In basemap, the sinusoidal projection is global, so we won't use it.
    # Instead we'll convert the grid back to lat/lons.
    #sinu = pyproj.Proj("+proj=sinu +R=6371007.181 +nadgrids=@null +wktext")
    #+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs
    #+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
    sinu = pyproj.Proj("+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
    wgs84 = pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs")
    lon, lat = pyproj.transform(sinu, wgs84, xv, yv)

    xmin = lon[-1,0]
    ymax = lat[0,-1]

    pixel_size = 0.05
    affine = Affine(pixel_size, 0, xmin,
                    0, -pixel_size, ymax)

    data_day = data_day * scale_factor
    data_night = data_night * scale_factor

    distancerasters.export_raster(data_day, affine, dayout, nodata=0)
    distancerasters.export_raster(data_night, affine, nightout, nodata=0)

