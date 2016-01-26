#!/bin/bash


# input and output directories
in_dir="/home/userz/globus-data/data/ltdr.nascom.nasa.gov/allData/Ver4/ndvi_raw"
out_dir="/home/userz/globus-data/data/ltdr.nascom.nasa.gov/allData/Ver4/ndvi"




# y="$in_dir"/1982
# year=`echo $y | sed 's/.*\///'`

# mkdir -p "$out_dir"/"$year"


# for d in "$y"/*; do

# 	day=`echo $d | sed 's/.*\///'`

# 	out_file="$out_dir"/"$year"/"$day"

# 	gdal_calc.py -A "$d" --outfile="$out_file" --calc="A*(A>=0)+(-9999)*(A<0)" --NoDataValue=-9999 --co COMPRESS=LZW --co BIGTIFF=IF_NEEDED

# done


for y in "$in_dir"/*; do

	year=`echo $y | sed 's/.*\///'`

	if [[ "$year" != "1981" && "$year" != "1982" ]]; then

		mkdir -p "$out_dir"/"$year"

		parallel -q gdal_calc.py -A {}  --outfile="$out_dir"/"$year"/{/} --calc="A*(A>=0)+(-9999)*(A<0)" --NoDataValue=-9999 --co COMPRESS=LZW --co BIGTIFF=IF_NEEDED ::: "$y"/*

	fi

done


# for y in "$in_dir"/*; do

# 	year=`echo $y | sed 's/.*\///'`

# 	mkdir -p "$out_dir"/"$year"

# 	for d in "$y"/*; do

# 		day=`echo $d | sed 's/.*\///'`

# 		out_file="$out_dir"/"$year"/"$day"

# 		gdal_calc.py -A "$d" --outfile="$out_file" --calc="A*(A>=0)+(-9999)*(A<0)" --NoDataValue=-9999 --co COMPRESS=LZW --co BIGTIFF=IF_NEEDED

# 	done

# done


