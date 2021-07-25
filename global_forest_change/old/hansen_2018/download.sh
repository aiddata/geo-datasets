#!/bin/bash


group_list=(
    treecover2000
    gain
    datamask
    lossyear
)

link_base_url="https://storage.googleapis.com/earthenginepartners-hansen/GFC-2018-v1.6"
raw_dir='/sciclone/aiddata10/REU/geo/raw/hansen/GFC-2018-v1.6/tiles'

for i in ${group_list[*]}; do
    echo $i
    tmp_dir=${raw_dir}/${i}
    mkdir -p ${tmp_dir}
    wget -c -N -P ${tmp_dir} -i ${link_base_url}/${i}.txt
done


# data_dir='/sciclone/aiddata10/REU/geo/data/rasters/hansen/GFC2018/'${i}
# mkdir -p ${data_dir}
