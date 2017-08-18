
Files:


download.sh - download all raw data from source (compressed)

extract.sh - unzip desired files from raw data

viirs_data_filter.py - filter/prepare raw monthly data tiles
jobscript_mosaic - used to run viirs_data_filter.py (parallel job on sciclone)

viirs_mosaic.py - mosaic filtered monthly tiles (result of viirs_data_filter.py)
jobscript_mosaic - used to run viirs_mosaic.py (parallel job on sciclone)

viirs_yearly.py - creates yearly aggregates from filter monthly data (result of viirs_data_filter.py) and mosaics
jobscript_yearly - used to run viirs_yearly.py (parallel job on sciclone)
