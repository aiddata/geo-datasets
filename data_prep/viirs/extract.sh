#!/bin/bash



src="/sciclone/aiddata10/REU/geo/raw/viirs/source/dnb_composites/v10"

dst="/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/raw_monthly"

cd $src


find -maxdepth 1 -type d -regex "./[2][0-9][0-9][0-9][0-1][0-9]$" -print0 |
while read -d $'\0' folder
do

    month=$(basename "$folder")
    echo $month

    mkdir -p $dst/$month

    find $src/$month/vcmcfg -maxdepth 1 -name \*tgz -exec tar -zxv -C $dst/$month -f {} \;

done

