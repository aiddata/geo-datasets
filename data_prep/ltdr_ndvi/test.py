"""
unpack a single subdataset from a HDF container, reprojectm and write to GeoTiff

Parts of code pulled from:

https://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
https://gis.stackexchange.com/questions/42584/how-to-call-gdal-translate-from-python-code
https://stackoverflow.com/questions/10454316/how-to-project-and-resample-a-grid-to-match-another-grid-with-gdal-python/10538634#10538634
https://jgomezdans.github.io/gdal_notes/reprojection.html

Notes:

Rebuilding geotransform is not really necessary in this case but might
be useful for future data prep scripts that can use this as startng point.

"""
import os
import numpy as np
from osgeo import gdal, osr


hdf_file = "/home/userw/Desktop/AVH13C1.A2010153.N19.004.2015206181729.hdf"
dst_dir = "/home/userw/Desktop"
subdataset = 0
band_path = os.path.join(
    dst_dir,
    "{0}-sd{1}.tif".format(
        os.path.basename(os.path.splitext(hdf_file)[0]),
        subdataset + 1
    )
)

# -----------------------------------------------------------------------------

# open the dataset
hdf_ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)
band_ds = gdal.Open(hdf_ds.GetSubDatasets()[subdataset][0], gdal.GA_ReadOnly)

# -----------------------------------------------------------------------------

src_proj = osr.SpatialReference()
src_proj.ImportFromWkt(band_ds.GetProjection())

dst_proj = osr.SpatialReference()
dst_proj.ImportFromEPSG(4326)

tx = osr.CoordinateTransformation(src_proj, dst_proj)

src_gt = band_ds.GetGeoTransform()
pixel_xsize = src_gt[1]
pixel_ysize = abs(src_gt[5])

(ulx, uly, ulz ) = tx.TransformPoint(src_gt[0], src_gt[3])

(lrx, lry, lrz ) = tx.TransformPoint(src_gt[0] + src_gt[1]*band_ds.RasterXSize,
                                     src_gt[3] + src_gt[5]*band_ds.RasterYSize)

# Calculate the new geotransform
dst_gt = (ulx, pixel_xsize, src_gt[2],
           uly, src_gt[4], -pixel_ysize)

# -----------------------------------------------------------------------------


driver = gdal.GetDriverByName('GTiff')
out_ds = driver.Create(
    band_path,
    int((lrx - ulx)/pixel_xsize),
    int((uly - lry)/pixel_ysize),
    1,
    gdal.GDT_Int16
)


# -----------------------------------------------------------------------------

out_ds.SetGeoTransform(dst_gt)
out_ds.SetProjection(dst_proj.ExportToWkt())

# need to test different resampling methods
# (nearest is default, probably used by gdal_translate)
# do not actually  think it matters for this case though
# as there does not seem to be much if any need for r
# resampling when reprojecting between these projections
gdal.ReprojectImage(band_ds, out_ds,
                    src_proj.ExportToWkt(), dst_proj.ExportToWkt(),
                    gdal.GRA_Bilinear)


band_array = out_ds.ReadAsArray().astype(np.int16)
band_array[np.where(band_array < 0)] = -9999

out_ds.GetRasterBand(1).WriteArray(band_array)
out_ds.GetRasterBand(1).SetNoDataValue(-9999)

# out_ds.GetRasterBand(1).ReadAsArray()

# -----------------------------------------------------------------------------

hdf_ds = None
band_ds = None
out_ds = None


