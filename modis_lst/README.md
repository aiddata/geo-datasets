# MODIS/Terra land surface temperature and emissivity daily products
Downloaded from:https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mod11c1_v006

- Register an account, and use the account information to access data from data pool
- Download data:
    -  Run data_download.py to create a filelist.txt which saves each file directory on the server;
    -  wget -L --user=youraccountemail --password=yourpassword --load-cookies ~/.cookies --save-cookies ~/.cookies -i filelist.txt (the password has to be set in the command line);
    -  The raw hdf data file will be downloaded in the destination directory;
- Data extraction:
    - run data_processing.R to extract "Monthly daytime 3min CMG Land-surface Temperature" and "Monthly nighttime 3min CMG Land-surface Temperature"
    - See the hdf layer descriptions from: https://icess.eri.ucsb.edu/modis/LstUsrGuide/usrguide_month_cmg.html
    - Local attributes can be found in Table 4: https://icess.eri.ucsb.edu/modis/LstUsrGuide/usrguide_mod11.html#Table_4
    - Spatial unit: 0.05 degree latitude/longitude grids
    - value unit: Kelvin
    - The effective calibration formula for the "LST" SDS is LST = the SDS data in uint16 * 0.02, giving a value in the range of 150-1310.7K.

- Raw data products are in: /sciclone/aiddata10/REU/pre_geo/modis_temp/rawdata
- The final products are saved in: /sciclone/aiddata10/REU/pre_geo/modis_temp/temp