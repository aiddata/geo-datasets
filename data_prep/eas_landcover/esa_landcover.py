

import rasterio


intif = r"/sciclone/home10/zlv/datasets/esa_lc/ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992_2015-v2.0.7.tif"
outdir = r"/sciclone/data20/zlv/esa_lc"



with rasterio.open(intif) as tifs:

    profile = tifs.profile

    profile.update(count=1)

    start_year = 1992

    for band in range(1, 25, 1):

        array = tifs.read(band)

        output = "{0}/esa_lc_{1}.tif".format(outdir, start_year)

        start_year += 1

        print "write raster for year: ", start_year

        with rasterio.open(output, 'w', **profile) as dst:
            dst.write(array, 1)

