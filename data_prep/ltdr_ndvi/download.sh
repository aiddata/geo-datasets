#!/bin/bash

# download all ltdr data (ver4)
wget -r -c -N -P /sciclone/aiddata10/REU/geo/raw/ltdr --retr-symlinks=yes ftp://ltdr.nascom.nasa.gov/allData/Ver4/
