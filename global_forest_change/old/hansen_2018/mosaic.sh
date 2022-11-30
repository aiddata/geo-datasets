#!/bin/bash


group=$1

echo ${group}

raw_dir='/sciclone/aiddata10/REU/geo/raw/hansen/GFC-2018-v1.6/tiles/'${group}
tmp_dir='/sciclone/aiddata10/REU/geo/raw/hansen/GFC-2018-v1.6/mosaic'
data_dir='/sciclone/aiddata10/REU/geo/data/rasters/hansen/GFC-2018-v1.6/'${group}

mkdir -p ${tmp_dir}
mkdir -p ${data_dir}

# mosaic tiles
gdal_merge.py -of GTiff -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES ${raw_dir}/*.tif -o ${tmp_dir}/${group}.tif

# copy to data dir
cp ${tmp_dir}/${group}.tif ${data_dir}/${group}.tif
