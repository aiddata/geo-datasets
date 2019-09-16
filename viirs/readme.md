
Info:

viirs = Visible Infrared Imaging Radiometer Suite
dnb = Day/Night Band
vcm = viirs cloud mask
sl = stray light
cfg = cloud free grids

vcmcfg - excludes any data contaminated by stray light
vcmslcfg - data impacted by stray light are corrected but not removed

---------------------------------------

Instructions:

Step 1)
- Script: download.sh
- Description: Downloads all raw data from source (compressed) for specified years. Downloads both vcmcfg and vcmslcfg
    + set start and end year in script
    + run script

Step 2)
- Script: extract.sh
- Description: unzip desired files from raw data
    + set start and end year in script
    + run script

Step 3)
- Script: viirs_data_filter.py
- Description: filter/prepare raw monthly data tiles
    + Set list of all years to process (can be int or str)
    + Set value for minimum cloud free day threshold
    + Set mode (serial or parallel)
    + Edit jobscript_mosaic based on resources needed
    + Run jobscript_mosaic (qsub jobscript_data_filter)

Step 4)
- Script: viirs_mosaic.py
- Description: mosaic filtered monthly tiles (result of viirs_data_filter.py)
    + Set years (must be list of integers)
    + Set mode (serial or parallel)
    + Edit jobscript_mosaic based on resources needed
    + Run jobscript_mosaic (qsub jobscript_mosaic)

Step 5)
- Script: viirs_yearly.py
- Description: creates yearly aggregates from filter monthly data (result of viirs_data_filter.py) and then mosaics
    + Set years (must be list of integers)
    + Set aggregation method (default: max)
    + Set run_agg and run_mosaic boolean variables
    + Set mode (serial or parallel)
    + Edit jobscript_mosaic based on resources needed
    + Run jobscript_yearly (qsub jobscript_yearly)
