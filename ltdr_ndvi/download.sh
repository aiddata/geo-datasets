#!/bin/bash

# downloads ltdr ndvi data (avhrr sensor) from ladsweb using an app key (replaced with tokens)
# which you can get by creating a user account

# wget should check from existing data before downloading so it will not
# redownload everything each time it runs
#
# however, it will still take some time to run as it checks for exclusions and
# what needs to be downloaded

# a year can be added as a subdir of a sensor (assuming the sensor/year pair exists)
# to download only a specific subset of the data more quickly
#
# for ongoing updates (2020+) N19_AVH13C1 is the current sensor which can be specificied
# when trying to do quicker downloads

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
