#!/bin/bash

# get water feature data and add to aiddata10 raw data directory


# -----------------------------------------------------------------------------


# download (from ftp) and unzip specified version of GSHHG database

version="2.3.6"
src="ftp://ftp.soest.hawaii.edu/gshhg/gshhg-shp-"${version}".zip"

dst="/sciclone/aiddata10/REU/raw/gshhg"

wget -cNv -P ${dst} ${src}

cd ${dst}
unzip ${dst}/$(basename ${src}) -d ${dst}/$(basename ${src} .zip)


# -----------------------------------------------------------------------------


# download (from git) and unzip specified commit for natural-earth-vector data

commit=d4533efe3715c55b51f49bc2bde9694bff2bf7b1
src2="https://github.com/nvkelso/natural-earth-vector/archive/"${commit}".zip"
dst2="/sciclone/aiddata10/REU/raw/natural-earth-vector"

wget -cNv -P ${dst2} ${src2}
mv ${dst2}/${commit} ${dst2}/${commit}.zip

cd ${dst2}
unzip ${dst2}/$(basename ${src2}) -d ${dst2}/$(basename ${src2} .zip)
mv ${commit} tmp_${commit}
mv ${commit}/* ${commit}
rm -r tmp_${commit}commit

# version2=$(cat ${dst2}/${commit}/natural-earth-vector-${commit}/VERSION)


# -----------------------------------------------------------------------------


# shorelines (land polygons, needs to be inverted)
# GSHHS_f_L1

# use natural earth lakes (very similar to WDB2 lakes - GSHHS_f_L2)

# use natural earth rivers (very similar to below combination of WDB2 river layers)

# Level  2: Permanent major rivers.
# WDBII_river_f_L02

# Level  3: Additional major rivers.
# WDBII_river_f_L03

# Level  6: Intermittent rivers - major.
# WDBII_river_f_L06

# Level  9: Major canals.
# WDBII_river_f_L09




