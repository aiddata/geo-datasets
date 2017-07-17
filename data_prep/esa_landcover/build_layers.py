"""

Download from and unzip multiband tif to input path specificed below

http://maps.elie.ucl.ac.be/CCI/viewer/download.php


qsub -I -l nodes=1:c18c:ppn=16 -l walltime=24:00:00
python ~/active/master/asdf-datasets/data_prep/esa_landcover/build_layers.py

"""

import rasterio


src_path = "/sciclone/aiddata10/REU/geo/raw/esa_landcover_v207/ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992_2015-v2.0.7.tif"
dst_dir_path = "/sciclone/aiddata10/REU/geo/data/rasters/esa_landcover_v207"


start_year = 1992

with rasterio.open(src_path) as src:

    profile = src.profile
    profile.update(count=1)

    # 24 years/bands
    for i in range(24):

        year = start_year + i
        band = i + 1

        print "Running year: {0} (band: {1})".format(year, band)

        array = src.read(band)

        dst_path = "{0}/esa_lc_{1}.tif".format(dst_dir_path, year)

        with rasterio.open(dst_path, 'w', **profile) as dst:
            dst.write(array, 1)

