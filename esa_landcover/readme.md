
Download from and unzip multiband tif to input path specificed below
http://maps.elie.ucl.ac.be/CCI/viewer/download.php


Source data is single tif containing 24 bands, where each band is a year.
These bands needs to be split into separate tif files

Run sciclone job to build individual rasters for each year:

qsub -I -l nodes=1:c18c:ppn=16 -l walltime=24:00:00
python /sciclone/aiddata10/geo/master/source/geo-datasets/esa_landcover/build_layers.py
