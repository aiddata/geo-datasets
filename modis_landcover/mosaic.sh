#!/bin/bash


# environmental variable used to set the amount of memory GDAL is allowed to use
# export GDAL_CACHEMAX=8191
# export GDAL_CACHEMAX=16383
# export GDAL_CACHEMAX=22527


src_root="/sciclone/aiddata10/REU/raw/ftp.glcf.umd.edu/glcf/Global_LNDCVR/UMD_TILES/Version_5.1"
dst_root="/sciclone/aiddata10/REU/data/rasters/external/global/glcf/modis_landcover_5.1"

mkdir -p ${dst_root}

for year_dir in $(find ${src_root} -mindepth 1 -maxdepth 1 -type d | sort -t '\0' -n); do

    out_name=$(basename ${year_dir})

    echo Building ${out_name}...

    for a in $(find ${year_dir} -name '*.gz'); do
        z=`echo $a | sed s/.gz//`
        gunzip -c $a > $z
    done

    mosaic_in=${year_dir}/*/*.tif
    mosaic_out="${dst_root}/${out_name}.tif"

    gdal_merge.py -of GTiff -init 255 -co TILED=YES -co BIGTIFF=YES ${mosaic_in} -o ${mosaic_out}
done

