#!/bin/bash

# process historic gimms ndvi data
# prereqs: GDAL (current version - 1.11.2)

# called by python mpi4py script
# inputs: 1) year, 2) month, 3) file

# internal variables:
# force 		- [bool] whether to force overwriting of existing mosaics
# GDAL_CACHEMAX - [int] environmental variable used to set the amount of memory GDAL is allowed to use
# myuser 		- [str] HPC account username used for writing to /local/scr on node

year=$1
month=$2
file=$3

force=1

export GDAL_CACHEMAX=8191
# export GDAL_CACHEMAX=10921
# export GDAL_CACHEMAX=12287
# export GDAL_CACHEMAX=16383
# export GDAL_CACHEMAX=22527

# update to use user's /local/scr directory on node
myuser="sgoodman"

# input and output directories
in_dir="/sciclone/aiddata10/REU/raw/historic_gimms_ndvi"
tmp_dir="/local/scr/"${myuser}"/REU/data/historic_gimms_ndvi/"${year}/${month}
out_dir="/sciclone/aiddata10/REU/data/historic_gimms_ndvi/"${year}


process_in="$in_dir"/"$file"
process_tmp="$tmp_dir"/`echo $file | sed s/.asc/.tif/`
process_act="$out_dir"/`echo $file | sed s/.asc/.tif/`


if [[ -f "$process_act" && $force -eq 0 ]]; then
	echo "$year"_"$month" exists
	exit 0
fi


# clean up tmp directories if they exists
\rm -f -r "$tmp_dir"

# remove output file if it exists (force==1)
\rm -f "$process_act"

# make tmp directory
mkdir -p "$tmp_dir"
	

# process - everything < 0 grouped into nodata value of -99
gdal_calc.py -A "$process_in" --outfile="$process_tmp" --calc="A*(A>=0)+(255)*(A<0)" --NoDataValue=255 --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES



function copy_output {

	if [[ (! -f "$process_act" || $( diff "$process_tmp" "$process_act" ) != "") && $out_attempt -lt 3 ]]; then

		cp "$process_tmp" "$process_act"
		((out_attempt+=1))
		copy_output

	elif [[ (! -f "$process_act" || $( diff "$process_tmp" "$process_act" ) != "") && $out_attempt -ge 3 ]]; then

		echo "$year"_"$month" failed to copy to output directory
		exit 2

	else

		# clean up tmp_dir
		\rm -f -r "$tmp_dir"

		# echo year_day that was processed
		echo "$year"_"$month" completed
		exit 0

	fi
}

# make output directory and move from tmp_dir to out_dir
mkdir -p "$out_dir"
out_attempt=0
copy_output




# # make output directory and move from tmp_dir to out_dir
# mkdir -p "$out_dir"
# mv "$process_tmp" "$process_act"

# # clean up tmp_dir
# \rm -f -r "$tmp_dir"

# # echo year_day that was processed
# echo "$year"_"$month"
# exit 0