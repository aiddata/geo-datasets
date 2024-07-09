#!/bin/bash

# mosaic 8 day period GIMMS NDVI from MODIS Terra
# prereqs: GDAL (current version - 1.11.2)

# called by python mpi4py script
# inputs: 1) year, 2) day

# internal variables:
# force 		- [bool] whether to force overwriting of existing mosaics
# GDAL_CACHEMAX - [int] environmental variable used to set the amount of memory GDAL is allowed to use
# myuser 		- [str] HPC account username used for writing to /local/scr on node

year=$1
day=$2

force=1

# export GDAL_CACHEMAX=8191
# export GDAL_CACHEMAX=10921
# export GDAL_CACHEMAX=12287
# export GDAL_CACHEMAX=16383
export GDAL_CACHEMAX=22527

# update to use user's /local/scr directory on node
myuser="sgoodman"

# input and output directories
in_dir="/sciclone/aiddata10/REU/raw/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}
tmp_dir="/local/scr/"${myuser}"/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}/${day}
out_dir="/sciclone/aiddata10/REU/data/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"${year}

cd "$in_dir"

# get a file name for the output mosiac
# uses format of all files for that year / day period except without "xNNyNN" in the tile name
outname=$( find . -type f -iregex ".*[.]gz" | sed -n "1p" | sed s/.gz// | sed "s:x[0-9]\+y[0-9]\+[.]::g" | sed "s:^[.]/::g;s:^:mosaic.:g" )

# check if data exists based on whether a non empty outname was created
if [[ $outname = "" ]]; then
	echo "$year"_"$day" has no data
	exit 1
fi


mosaic_tmp="$tmp_dir"/tmp_"$outname"
mosaic_act="$out_dir"/"$outname"


if [[ -f "$mosaic_act" && $force -eq 0 ]]; then
	echo "$year"_"$day" exists
	exit 0
fi


# clean up tmp directories if they exists
\rm -f -r "$tmp_dir"

# remove output mosaic if it already exists (force == 1)
\rm -f "$mosaic_act"

# make tmp directories
mkdir -p "$tmp_dir"/unzip
mkdir -p "$tmp_dir"/prep


# move gzipped files to tmp dir then gunzip and process individual frames
cp *.gz "$tmp_dir"
cd "$tmp_dir"

for a in *.gz; do

	# gunzip
	z="$tmp_dir"/unzip/`echo $a | sed s/.gz//`
	gunzip -c $a > $z

	# process frame
	prep_tmp="$tmp_dir"/prep/`echo $a | sed s/.gz//`
	gdal_calc.py -A "$z" --outfile="$prep_tmp" --calc="A*(A<=250)+(255)*(A>250)" --NoDataValue=255

done


# merge processed frames into compressed geotiff mosaic
# nodata value of 255
cd "$tmp_dir"/prep
gdal_merge.py -of GTiff -init 255 -n 255 -a_nodata 255 -co COMPRESS=LZW -co TILED=YES -co BIGTIFF=YES *.tif -o "$mosaic_tmp"



function copy_output {

	if [[ (! -f "$mosaic_act" || $( diff "$mosaic_tmp" "$mosaic_act" ) != "") && $out_attempt -lt 3 ]]; then

		cp "$mosaic_tmp" "$mosaic_act"
		((out_attempt+=1))
		copy_output

	elif [[ (! -f "$mosaic_act" || $( diff "$mosaic_tmp" "$mosaic_act" ) != "") && $out_attempt -ge 3 ]]; then

		echo "$year"_"$day" failed to copy to output directory
		exit 2

	else

		# clean up tmp_dir
		\rm -f -r "$tmp_dir"

		# echo year_day that was processed
		echo "$year"_"$day" completed
		exit 0

	fi
}

# make output directory and move from tmp_dir to out_dir
mkdir -p "$out_dir"
out_attempt=0
copy_output



# # make output directory and move from tmp_dir to out_dir
# mkdir -p "$out_dir"
# mv "$mosaic_tmp" "$mosaic_act"

# # clean up tmp_dir
# \rm -f -r "$tmp_dir"

# # echo year_day that was processed
# echo "$year"_"$day" completed
# exit 0
