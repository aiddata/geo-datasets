#!/bin/bash


# cru ts version 4.05
base_url="https://crudata.uea.ac.uk/cru/data/hrg/cru_ts_4.05/cruts.2103051243.v4.05"

dst="/sciclone/aiddata10/REU/geo/raw/cru_ts_4.05"

mkdir -p ${dst}

variables=(
    # cld
    # dtr
    # frs
    # pet
    pre
    # tmn
    tmp
    # tmx
    # vap
    # wet
)

for v in "${variables[@]}"; do
	src=${base_url}/${v}/cru_ts4.05.1901.2020.${v}.dat.nc.gz
    echo $src
    wget --timestamping ${src} -P ${dst}
    gunzip -c ${dst}/cru_ts4.05.1901.2020.${v}.dat.nc.gz > ${dst}/cru_ts4.05.1901.2020.${v}.dat.nc
done
