#!/bin/bash

# get water feature data and add to aiddata10 raw data directory


# -----------------------------------------------------------------------------


# download (from ftp) and unzip specified version of GSHHG database

version="2.3.7"
src="ftp://ftp.soest.hawaii.edu/gshhg/gshhg-shp-"${version}".zip"

dst="/sciclone/aiddata10/REU/geo/raw/gshhg"

wget -cNv -P ${dst} ${src}

cd ${dst}
unzip ${dst}/$(basename ${src}) -d ${dst}/$(basename ${src} .zip)





