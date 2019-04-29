#!/bin/bash

app_key="PUT YOUR APP KEY HERE"

base_url="https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/465"

dst="/sciclone/aiddata10/REU/geo/raw/ltdr/LAADS"

sensors=(
    N07_AVH13C1
    N09_AVH13C1
    N11_AVH13C1
    N14_AVH13C1
    N16_AVH13C1
    N18_AVH13C1
    N19_AVH13C1
)

for s in "${sensors[@]}"; do
	src=${base_url}/$s
    echo $src
    wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=2 ${src} --header "Authorization: Bearer ${app_key}" -P ${dst}
done
