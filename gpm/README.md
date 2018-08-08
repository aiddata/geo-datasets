# GPM precipitation

Data process:
- Run data_download.py to download gpm monthly data
- Run data_processing.py to rename the monthly data

to get yearly aggregation dataset:
- run build_monthly.py


Notes:
- The GPM was launched in Feb 2014, and the montly imerg data starts at March 2014.
- Data information: https://pmm.nasa.gov/data-access/downloads/gpm
- Data Readme: https://pps.gsfc.nasa.gov/Documents/README.GIS.pdf
- Data download:ftp://arthurhou.pps.eosdis.nasa.gov/gpmdata/
- Montly unit is 0.001 millimeters per hour(monthly average precipitation rate were scaled up by 1000), 30 minutes and 1 day unit are 0.1 millimeters per hour.
- WARNING: as of 2018-07-26 download, the data (up to 2018-01-01 data) the data still uses a nodata value of 9999 even though documentation states they switched to
           nodata value of 29999 after 2017.

There are two types of monthly precipitation data:
- gis version: this includes .tif .tfw and .zip of monthly. The .tif is montly total precipitation.
- imerg version includes hdf5 of total precipitation, liquid/mixed precipitation, liquid-equivalent ice phase only precipitation, and the precent of liquid/mixed phase vs total precipitation data.
- raw data: /sciclone/aiddata10/REU/pre_geo/GPM/raw
- renamed GPM data: /sciclone/aiddata10/REU/pre_geo/GPM/processed
