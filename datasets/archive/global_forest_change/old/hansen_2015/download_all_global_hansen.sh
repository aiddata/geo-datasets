#!/bin/bash

# data web source:
#   https://earthenginepartners.appspot.com/science-2013-global-forest/download_v1.2.html



group_list=(
    treecover2000
    loss
    gain
    lossyear
    datamask
)


for i in ${group_list[*]}; do 

    echo $i

    raw_dir='/sciclone/aiddata10/REU/raw/hansen/GFC2015/'${i}
    data_dir='/sciclone/aiddata10/REU/data/rasters/external/global/hansen/GFC2015/'${i}

    mkdir -p ${raw_dir}'/tiles'
    mkdir -p ${data_dir}

    wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/${i}/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/${i}.txt

done


# wget tiles

# wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/GFC2015/treecover2000/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/treecover2000.txt
# wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/GFC2015/loss/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/loss.txt
# wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/GFC2015/gain/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/gain.txt
# wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/GFC2015/lossyear/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/lossyear.txt
# wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/GFC2015/datamask/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/datamask.txt


# echo $group_list | xargs -I {} -n 1 -P 10 wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/GFC2015/{}/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/{}.txt

# echo $group_list | parallel -j5 wget -c -N -P /sciclone/aiddata10/REU/raw/hansen/GFC2015/{}/tiles -i https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/{}.txt



# --------------------------------------------------


# file_list=(
#     https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/treecover2000.txt
#     https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/loss.txt
#     https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/gain.txt
#     https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/lossyear.txt
#     https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/datamask.txt
# )

# file_url=$1
# file_group=$(basename $file_url .txt) 
# echo ${file_group}

# # wget tiles
# wget -c -N -P ${raw_dir}'/tiles' -i $file_url

