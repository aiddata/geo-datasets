#!/bin/bash


src="https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/spectrometers-radiometers/imaging/viirs/dnb_composites/"

dst="/sciclone/aiddata10/REU/raw/viirs"

wget -rcNv -nH --cut-dirs=6 --level=0 -P ${dst} ${src}

