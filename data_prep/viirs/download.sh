#!/bin/bash


# dst="/sciclone/aiddata10/REU/raw/viirs/"

# src="https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/spectrometers-radiometers/imaging/viirs/dnb_composites/v10"

# wget -r -c -N -nH --cut-dirs=6 --level=0 --accept tgz -P ${dst} ${src}



dst="/sciclone/aiddata10/REU/raw/viirs/dnb_composites"

src_base="https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/spectrometers-radiometers/imaging/viirs/dnb_composites/v10"

start_year=2012
end_year=2017
for y in $(seq $start_year $end_year); do

    for m in $(seq 1 12); do
        m=$(printf %02d $m)
        echo $y$m

        src_a=${src_base}/$y$m/vcmcfg
        src_b=${src_base}/$y$m/vcmslcfg

        wget --no-check-certificate -r -c -N -nH --cut-dirs=7 --level=0 -A "*.tgz" -P ${dst} ${src_a}
        wget --no-check-certificate -r -c -N -nH --cut-dirs=7 --level=0 -A "*.tgz" -P ${dst} ${src_b}

    done

done


