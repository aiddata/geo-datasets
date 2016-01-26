#!/bin/bash

# converts hdf file to geotiff and reprojects from EPSG:4008 to EPSG:4326

# example gdal_translate
# gdal_translate -a_srs EPSG:4326 -co COMPRESS=LZW -co BIGTIFF=IF_NEEDED -of GTiff 
# 			HDF4_EOS:EOS_GRID:"/source/path/AVH13C1.A2005330.N16.004.2014115195505.hdf":Grid:NDVI 
# 			/path/to/output.tif

sensor=$1
filename=$2
year=$3
day=$4


force=1


# update to use user's /local/scr directory on node
myuser="sgoodman"

# input, tmp, output files
in_file="/sciclone/aiddata10/REU/raw/ltdr.nascom.nasa.gov/allData/Ver4/"${sensor}/${year}/${filename}.hdf
tmp_file="/local/scr/"${myuser}"/REU/data/ltdr.nascom.nasa.gov/allData/Ver4/"${sensor}/${year}/${filename}.tif
out_file="/sciclone/aiddata10/REU/data/ltdr.nascom.nasa.gov/allData/Ver4/"${sensor}/${year}/${filename}.tif


gdal_translate -a_srs EPSG:4326 -co COMPRESS=LZW -co BIGTIFF=IF_NEEDED -of GTiff HDF4_EOS:EOS_GRID:\""$in_file"\":Grid:NDVI "$tmp_file"

cp "$tmp_file" "$out_file"

\rm -f "$tmp_file"


# gdal_translate -a_srs EPSG:4326 -co COMPRESS=LZW -co BIGTIFF=IF_NEEDED -of GTiff HDF4_EOS:EOS_GRID:"/sciclone/aiddata10/REU/raw/ltdr.nascom.nasa.gov/allData/Ver4/N07/1982/AVH13C1.A1982089.N07.004.2013223195333.hdf":Grid:NDVI output_hdf_test.tif
# gdal_translate -a_srs EPSG:4326 -co COMPRESS=LZW -co BIGTIFF=IF_NEEDED -of GTiff HDF4_EOS:EOS_GRID:"/home/userz/globus-data/ltdr.nascom.nasa.gov/allData/Ver4/N07/1981/AVH13C1.A1981316.N07.004.2013228224007.hdf":Grid:NDVI /home/userz/Desktop/hdf_out.tif

# for f in $(find ./N11/*/AVH09C1*); do rm "$f"; done
# for f in $(find ./N11/*/AVH02C1*); do rm "$f"; done
