#!/bin/bash


access_token=`python3 get_token.py`

years=(
    2012
    2013
    2014
    2015
    2016
    2017
    2018
    2019
    2020
)

out_dir="/sciclone/aiddata10/REU/geo/raw/viirs/eogdata"


# annual (non-tiled)
year_url="https://eogdata.mines.edu/nighttime_light/annual/v20"
for y in "${years[@]}"; do
    echo "${year_url}/${y}"
    wget -c -m -np -nH --cut-dirs=1 --header "Authorization: Bearer ${access_token}" -P ${out_dir} "${base_url}/${y}" -R .html -A .tif.gz
done


# # monthly (tiled)
# month_url="https://eogdata.mines.edu/nighttime_light/monthly/v10"
# for y in "${years[@]}"; do
#     for m in $(seq 1 12); do
#         m=$(printf %02d $m)
#         echo "${month_url}/${y}/${y}${m}"
#         wget -c -m -np -nH --cut-dirs=1 --header "Authorization: Bearer ${access_token}" -P ${out_dir} "${month_url}/${y}/${y}${m}/vcmcfg" -R .html -A .tgz
#     done
# done


# monthly (non-tiled)
month_url="https://eogdata.mines.edu/nighttime_light/monthly_notile/v10"
for y in "${years[@]}"; do
    access_token=`python3 get_token.py`
    for m in $(seq 1 12); do
        m=$(printf %02d $m)
        echo "${month_url}/${y}/${y}${m}"
        wget -c -m -np -nH --cut-dirs=1 --header "Authorization: Bearer ${access_token}" -P ${out_dir} "${month_url}/${y}/${y}${m}/" -R .html -A .avg_rade9h.masked.tif,.cf_cvg.tif,.cvg.tif
    done
done
