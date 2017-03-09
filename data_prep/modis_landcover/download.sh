#!/bin/bash



src="ftp://ftp.glcf.umd.edu/glcf/Global_LNDCVR" #/UMD_TILES/Version_5.1/"

dst="/sciclone/aiddata10/REU/raw/modis_landcover"

wget -rcNv --level=0 -P ${dst} ${src}


