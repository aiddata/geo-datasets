"""
python3

resolution: 5 arc-min (0.08333333333333333 decimal degrees)
projection: WGS84
extent: lat: 90S - 90N; lon: 180E - 180W

nodata value = -9

data shape: (26, 2160, 4320)

year x row x column

years: 1990 - 2015

"""

import os
import errno
import netCDF4
import rasterio
import numpy as np
from affine import Affine

def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

# raw data dir
raw_dir = "/sciclone/aiddata10/REU/geo/raw/kummu_gdp_hdi/doi_10.5061_dryad.dk1j0__v1"

# out_dir = "/sciclone/aiddata10/REU/geo/data/rasters/kummu_gdp_hdi"
out_dir = "/sciclone/aiddata10/REU/pre_geo/data/rasters/kummu_gdp_hdi"

# list of netcdf files for different datasets
# key is name of variable in netcdf format
datasets = {
    "GDP_per_capita_PPP": os.path.join(raw_dir, "GDP_per_capita_PPP_1990_2015_v2.nc"),
    "GDP_PPP": os.path.join(raw_dir, "GDP_PPP_1990_2015_5arcmin_v2.nc"),
    "HDI": os.path.join(raw_dir, "HDI_1990_2015_v2.nc")
}

years = range(1990, 2015+1)

# iterate over datasets
# for name, path in datasets.iteritems():
for name, path in datasets.items():

    # name = "GDP_per_capita_PPP"
    # path = datasets[name]

    nc = netCDF4.Dataset(path)
    var = nc.variables[name][:]
    data = var.data

    # iterate over years
    for i, year in enumerate(years):
    # i, year = 0, 1990
    # year_data = data[i]

        # define variables needed for raster export meta
        shape = year_data.shape
        pixel_size = 0.083333333
        xmin = -180
        ymax = 90
        affine = Affine(pixel_size, 0, xmin,
                        0, -pixel_size, ymax)

        meta = {
            'count': 1,
            'crs': {'init': 'epsg:4326'},
            'dtype': 'float32',
            'transform': affine,
            'driver': 'GTiff',
            'height': shape[0],
            'width': shape[1],
            'nodata': -9,
            # 'compress': 'lzw'
        }

        # prep data for export format (wrap in array)
        raster_out = np.array([year_data])

        # define output path
        out_path = os.path.join(out_dir, name, "{}_{}.tif".format(name, year))

        # create output dir
        make_dir(os.path.dirname(out_path))

        # write raster
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(raster_out)
