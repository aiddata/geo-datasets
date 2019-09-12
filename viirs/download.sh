#!/bin/bash

# download viirs monthly/yearly dnb composites
#   https://www.gnu.org/software/wget/manual/wget.html

dst_base="/sciclone/aiddata10/REU/geo/raw/viirs/source/dnb_composites/v10"


src_base="https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/spectrometers-radiometers/imaging/viirs/dnb_composites/v10"

start_year=2017
end_year=2018
for y in $(seq $start_year $end_year); do

    for m in $(seq 1 12); do
        m=$(printf %02d $m)
        echo $y$m

        dst_a=${dst_base}/$y$m/vcmcfg
        dst_b=${dst_base}/$y$m/vcmslcfg

        src_a=${src_base}/$y$m/vcmcfg/
        src_b=${src_base}/$y$m/vcmslcfg/

        echo $src_a
        wget -c --no-check-certificate -nH --cut-dirs=10  -r -np  -P ${dst_a} ${src_a} -A "*.tgz"

        echo $src_b
        wget -c --no-check-certificate -nH --cut-dirs=10  -r -np  -P ${dst_b} ${src_b} -A "*.tgz"

    done

done
