#!/bin/bash


branch=$1
version=$2

src="${HOME}"/active/"$branch"

# base=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
base=/sciclone/aiddata10/REU/raw

cd $base

version_dir='gadm'${version}

# raw_dir='raw/'${version_dir}
# data_dir='data/'${version_dir}

raw_dir=${version_dir}
data_dir='/sciclone/aiddata10/REU/data/boundaries/'${version_dir}

if [ ! -d $raw_dir ]; then
    echo 'Could not find download directory for GADM version' ${version}
    exit 1
fi


mkdir -p $data_dir

for i in $raw_dir/*.zip; do

    # echo $i
    unzip -n $i -d $data_dir

done

iso_start=$((${#data_dir} + 2))
iso_end=$(($iso_start + 2))

for i in $data_dir/*.shp; do

    # echo $i

    iso3=$(echo ${i} | cut -c ${iso_start}-${iso_end})
    # echo $iso3

    name=$(basename ${i} .shp)
    # echo $name

    mkdir -p $data_dir/$name

    # cp -u $data_dir/$name.* $data_dir/$name
    mv $data_dir/$name.* $data_dir/$name

    abs_path=$(readlink -f ${i})

    # add to asdf
    python "${src}"/asdf/src/add_gadm.py ${branch} ${abs_path} ${version} auto

done


