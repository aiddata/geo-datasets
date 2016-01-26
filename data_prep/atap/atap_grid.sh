#!/bin/bash

# create rasters from atap (air temperature and precipitation) data 
# atap source data is csv-like format 

# base path (data folder parent path)
base=$1

# data folder name
src=$2

# src=Global2011P
# src=Global2011T

# src=precip_2014
# src=air_temp_2014



out="$base"/"$src"_out
mkdir -p "$out"
cd "$out"

tmpvrt="$out"/tmp.vrt

for item in "$base"/"$src"/*;do

	file=`echo $item | sed 's/.*\///' | tr '.' '_'`
	year=`echo $item | sed 's/.*\.//'`

	echo "$item"
	echo "$file"
	echo "$year"

	tmpcsv="$out"/tmp.csv

	# convert csv-like data to true csv
	sed -e 's/^[ \t]*//' <"$item" | tr -s ' ' | tr ' ' ',' >"$tmpcsv"

	# iterate over fields for each month
	for x in $(seq 3 14);do


		# update tmp.vrt with field for current month
		cat > tmp.vrt <<-EOF
		<OGRVRTDataSource>
		    <OGRVRTLayer name="tmp">
		        <SrcDataSource>$tmpcsv</SrcDataSource> 
				<GeometryType>wkbPoint</GeometryType> 
				<LayerSRS>WGS84</LayerSRS>
				<GeometryField encoding="PointFromColumns" x="field_1" y="field_2" z="field_$x"/> 
		    </OGRVRTLayer>
		</OGRVRTDataSource>

		EOF

		# get actual month number from month field number
		m=$(($x - 2))

		if [[ "$m" -lt 10 ]];then
			m=0"$m"
		fi

		outpath="$out"/"$year"/
		mkdir -p "$outpath"

        echo -e "\n"Processing "$year"_"$m"

		# create raster from vrt
        tmptif="$out"/tmp.tif
		gdal_grid -of GTiff -l "tmp" "$tmpvrt" "$tmptif"
        
        # fix vrt issue
        out_raster="$outpath"/"$file"_"$m".tif
        gdalwarp -srcnodata -1 -dstnodata -1 "$tmptif" "$out_raster"

	done;

done;


\rm -f "$tmpcsv"
\rm -f "$tmpvrt"
\rm -f "$tmptif"
