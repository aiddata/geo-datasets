#!/bin/bash

base_dir='/sciclone/aiddata10/REU/data/rasters/external/global/hansen/GFC2015'



# 00forest25 = 1 if treecover2000 >= 25 else 0
a1=${base_dir}/treecover2000/treecover2000.tif
out1=${base_dir}/00forest25/00forest25.tif
mkdir $(dirname $out1)
gdal_calc.py -A "${a1}" --outfile="${out1}" --calc="A>=25" --type="Byte" --overwrite --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES 



# lossgain = loss + gain
a2=${base_dir}/loss/loss.tif
b2=${base_dir}/gain/gain.tif
out2=${base_dir}/lossgain/lossgain.tif
mkdir $(dirname $out2)
gdal_calc.py -A "${a2}" -B "${b2}" --outfile="${out2}" --calc="A+B" --type="Byte" --overwrite --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES



# loss25 = loss * 00forest25
a3=${base_dir}/loss/loss.tif
b3=${base_dir}/00forest25/00forest25.tif
out3=${base_dir}/loss25/loss25.tif
mkdir $(dirname $out3)
gdal_calc.py -A "${a3}" -B "${b3}" --outfile="${out3}" --calc="A*B" --type="Byte" --overwrite --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES



# lossyr25 = lossyear * 00forest25
a4=${base_dir}/lossyear/lossyear.tif
b4=${base_dir}/00forest25/00forest25.tif
out4=${base_dir}/lossyr25/lossyr25.tif
mkdir $(dirname $out4)
gdal_calc.py -A "${a4}" -B "${b4}" --outfile="${out4}" --calc="A*B" --type="Byte" --overwrite --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES


# --------------------------------------------------

# - check that loss25 is same as loss 
# (should be, since loss should never have occured on area that wasn't at least 25% cover, by definition 
# ; unless standing replacement distrubance includes areas <25%). 
# can check by masking loss layer to make all cells >25% cover 0 and seeing if any are still 1 afterwards (see 'otherloss')

# otherloss = !00forest25 * loss
a5=${base_dir}/00forest25/00forest25.tif
b5=${base_dir}/loss/loss.tif
out5=${base_dir}/otherloss/otherloss.tif
mkdir $(dirname $out5)
gdal_calc.py -A "${a5}" -B "${b5}" --outfile="${out5}" --calc="(A==0)*B" --type="Byte" --overwrite --co COMPRESS=LZW --co TILED=YES --co BIGTIFF=YES


