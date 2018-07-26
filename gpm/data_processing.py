

import os
import errno
import rasterio
import numpy as np


def make_dir(path):
    """Make directory.

    Args:
        path (str): absolute path for directory

    Raise error if error other than directory exists occurs.
    """
    if path != '':
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise


def export_raster(raster, affine, path, out_dtype='float64', nodata=None):
    """Export raster array to geotiff
    """
    # affine takes upper left
    # (writing to asc directly used lower left)
    meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'dtype': out_dtype,
        'affine': affine,
        'driver': 'GTiff',
        'height': raster.shape[0],
        'width': raster.shape[1],
        # 'nodata': -1,
        # 'compress': 'lzw'
    }

    if nodata is not None:
        meta['nodata'] = nodata

    raster_out = np.array([raster.astype(out_dtype)])

    make_dir(os.path.dirname(path))

    # write geotif file
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(raster_out)




input_dir = "/sciclone/aiddata10/REU/geo/raw/gpm"

output_dir = "/sciclone/aiddata10/REU/geo/data/rasters/gpm/monthly"

file_list = sorted([f for f in os.listdir(input_dir) if f.endswith("tif")])



for f in file_list:

    input_file = os.path.join(input_dir, f)

    date = f.split(".")[4].split("-")[0]
    year = date[:4]
    month = str(date[4:6])
    name = "gpm_precipitation_%s_%s.tif"%(year,month)

    print "Processing {}".format(name)

    output_file = os.path.join(output_dir, name)

    src = rasterio.open(input_file)
    data = src.read(1)
    export_raster(data, src.affine, output_file, out_dtype=str(data.dtype), nodata=9999)
