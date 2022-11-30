#!/bin/bash


group=$1

echo ${group}

raw_dir='/sciclone/aiddata10/REU/pre_geo/hansen_2017/'${group}
data_dir='/sciclone/aiddata10/REU/pre_geo/hansen_2017/'${group}

#mkdir -p ${data_dir}

# mosaic tiles
gdal_merge.py -of GTiff -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES ${raw_dir}/tiles/*.tif -o ${raw_dir}/${group}.tif

# copy to data dir
#cp ${raw_dir}/${group}.tif ${data_dir}/${group}.tif

