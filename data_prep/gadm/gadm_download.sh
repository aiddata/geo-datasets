#!/bin/bash

# downloads gadm shapefiles based on user specified version
#
#
# download page
#   http://www.gadm.org/country
#
# download format
#   http://biogeo.ucdavis.edu/data/gadm`version`/shp/`iso3`_adm_shp.zip
#
# country iso3 lookup
#   https://www.iso.org/obp/ui/#search
#
#
# gadm iso3 list was pulled from GADM download page html source
#   - currently (20160325, v2.8) includes 5 other ISO3 in
#     addition to the standard 249 countries




# input gadm version (eg: 2.8)
version=$1

if [ $version == "" ]; then
    echo 'No GADM version provided'
    exit 1
fi

url="http://biogeo.ucdavis.edu/data/gadm"${version}

if ! curl --output /dev/null --silent --head --fail ${url}; then
    echo 'GADM version' ${version} 'not found'
    exit 1
else
    echo 'Downloading GADM version' ${version}
fi

current=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${current}
iso3=($(cat gadm_iso3.txt))


# base=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
base=/sciclone/aiddata10/REU/raw
cd $base

# raw_dir='raw/gadm'${version}
raw_dir='gadm'${version}

mkdir -p $raw_dir



# wget -cNO tmp.gadm.html http://www.gadm.org/country


for i in ${iso3[*]}; do

    # echo $i
    file="http://biogeo.ucdavis.edu/data/gadm"${version}"/gpkg/"${i}"_adm_gpkg.zip"

    # echo $file
    wget -c -N -P $raw_dir $file

done
