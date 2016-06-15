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
data_dir='/sciclone/aiddata10/REU/data/boundaries_test/'${version_dir}

if [ ! -d $raw_dir ]; then
    echo 'Could not find download directory for GADM version' ${version}
    exit 1
fi

mkdir -p $data_dir


iso_start=$((${#data_dir} + 2))
iso_end=$(($iso_start + 2))


# for i in $raw_dir/*_gpkg.zip; do

#     # echo $i
#     unzip -n $i -d $data_dir

# done

for i in $data_dir/*.gpkg; do

    # echo $i

    iso3=$(echo ${i} | cut -c ${iso_start}-${iso_end})
    echo $iso3

    # name=$(basename ${i} .gpkg)
    # # echo $name

    layers=$(ogrinfo "$i" -so | grep '.: '${iso3}'_adm. ')
    # echo "$layers"

    echo "$layers" | while read -r line; do

        # echo $line

        layer=$(echo $line | cut -c 4-11)
        echo $layer

        bnd_dir=$data_dir/$layer
        mkdir -p $bnd_dir

        layer_file=$bnd_dir/$layer.geojson
        ogr2ogr -f GeoJSON $layer_file $i $layer

    done

    # rm $i

done


