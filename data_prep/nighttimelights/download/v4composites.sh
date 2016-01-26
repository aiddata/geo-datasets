#!/bin/bash

# download night time light (v4) raw tar files

# local downdload to issues with sciclone
# then using globus connect to move raw download to sciclone

# -------------------------
# init

# raw_dir=/sciclone/aiddata10/REU/raw/v4avg_lights_x_pct

raw_dir=/sciclone/aiddata10/REU/raw/v4composites
mkdir -p ${raw_dir}

cd ${raw_dir}


# -------------------------
# files download

z=(F101992 F101993 
    F121994 F121995 F121996 
    F141997 F141998 F141999 F152000 F152001 F152002 F152003
    F162004 F162005 F162006 F162007 F162008 F162009
    F182010 F182011 F182012)

for i in ${z[*]}; do 

    echo $i
    file="http://ngdc.noaa.gov/eog/data/web_data/v4composites/"${i}".v4.tar"
    echo $file
    wget -c -N $file

done



# downloaded files must be unpacked with tar 
# the unpackage tif files must then be decompressed with gunzip

data_dir=/sciclone/aiddata10/REU/data/rasters/external/global/v4composites
mkdir -p ${data_dir}

for i in *; do

    name=`echo $i | sed s/.tar//`
    mkdir ${name}
    tar -xvf ${raw_dir}/${i} -C ${name}
    gunzip ${name}/*stable_lights.avg_vis.tif.gz
    mv ${name}/*stable_lights.avg_vis.* ${data_dir}
    rm -r ${name}

done

