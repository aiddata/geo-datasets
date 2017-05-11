

from osgeo import gdal
import numpy as np



# -----------------------------------------------------------------------------
# https://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
# also: https://gis.stackexchange.com/questions/42584/how-to-call-gdal-translate-from-python-code

hdf_file = ""
dst_dir = ""
subdataset = ""

"""unpack a single subdataset from a HDF5 container and write to GeoTiff"""
# open the dataset
hdf_ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)
band_ds = gdal.Open(hdf_ds.GetSubDatasets()[subdataset][0], gdal.GA_ReadOnly)




# build output path
band_path = os.path.join(dst_dir, os.path.basename(os.path.splitext(hdf_file)[0]) + "-sd" + str(subdataset+1) + ".tif")

# write raster
driver = gdal.GetDriverByName('GTiff')

# -----------------
# method 1

# read into numpy array
band_array = band_ds.ReadAsArray().astype(np.int16)

out_ds = driver.Create(
    band_path,
    band_ds.RasterXSize,
    band_ds.RasterYSize,
    1,  #Number of bands
    gdal.GDT_Int16,
    ['COMPRESS=LZW', 'TILED=YES']
)

out_ds.SetGeoTransform(band_ds.GetGeoTransform())
out_ds.SetProjection(band_ds.GetProjection())
out_ds.GetRasterBand(1).WriteArray(band_array)
out_ds.GetRasterBand(1).SetNoDataValue(-32768)


# -----------------
# method 2

out_ds = driver.CreateCopy( band_path, band_ds, 0 )

# -----------------

hdf_ds = None
band_ds = None
out_ds = None  #close dataset to write to disc




# -----------------------------------------------------------------------------
# https://stackoverflow.com/questions/10454316/how-to-project-and-resample-a-grid-to-match-another-grid-with-gdal-python/10538634#10538634


# Source
src_filename = 'MENHMAgome01_8301/mllw.gtx'
src = gdal.Open(src_filename, gdal.GA_ReadOnly)
src_proj = src.GetProjection()
src_geotrans = src.GetGeoTransform()


# We want a section of source that matches this:
match_filename = 'F00574_MB_2m_MLLW_2of3.bag'
match_ds = gdal.Open(match_filename, gdal.GA_ReadOnly)
match_proj = match_ds.GetProjection()
match_geotrans = match_ds.GetGeoTransform()
wide = match_ds.RasterXSize
high = match_ds.RasterYSize

# Output / destination
dst_filename = 'F00574_MB_2m_MLLW_2of3_mllw_offset.tif'
dst = gdal.GetDriverByName('GTiff').Create(dst_filename, wide, high, 1, gdal.GDT_Float32)
dst.SetGeoTransform( match_geotrans )
dst.SetProjection( match_proj)


# Do the work

# need to test different resampling methods (nearest is default, probably used by gdal_translate)
gdal.ReprojectImage(src, dst, src_proj, match_proj, gdal.GRA_Bilinear)

