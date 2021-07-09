#!/bin/bash

# unzip monthly data from viirs dnb composite downloads
# ignores yearly data via regex on files
# only unzips vcmcfg (no stray light correction)

src="/sciclone/aiddata10/REU/geo/raw/viirs/source/dnb_composites/v10"

dst="/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/raw_monthly"

cd $src


start_year=2017
end_year=2018

for y in $(seq $start_year $end_year); do

    find -maxdepth 1 -type d -regex "./${y}[0-1][0-9]$" -print0 |
    while read -d $'\0' folder
    do

        month=$(basename "$folder")
        echo $month

        mkdir -p $dst/$month

        find $src/$month/vcmcfg -maxdepth 1 -name \*tgz -exec tar -zxv -C $dst/$month -f {} \;

    done

done
