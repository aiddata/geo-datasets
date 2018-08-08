# MODIS/Terra land surface temperature and emissivity daily products


Downloaded from:
https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mod11c1_v006

- Register an account, and use the account information to access data from data pool
- Download data:
    -  Run data_download.py to create a filelist.txt which saves each file directory on the server;
    -  wget -L --user=youraccountemail --password=yourpassword --load-cookies ~/.cookies --save-cookies ~/.cookies -i filelist.txt (the password has to be set in the command line);
    -  The raw hdf data file will be downloaded in the destination directory;


- Data extraction in python:
    - run data_processing.py to extract "Monthly daytime 3min CMG Land-surface Temperature" and "Monthly nighttime 3min CMG Land-surface Temperature"
    - Multiply scale_factor in python.



Data Products:
- Raw data products are in: /sciclone/aiddata10/REU/geo/raw/modis_lst
- The final products are saved in: /sciclone/aiddata10/REU/geo/data/rasters/modis_lst
- HDF manual: https://support.hdfgroup.org/release4/doc/UG_PDF.pdf

Data Aggregation to annual level:
- run build_monthly.py




=======================================

Unused stuff:

- projection: http://spatialreference.org/ref/sr-org/modis-sinusoidal-3/
- modis sinusoidal: https://modis-land.gsfc.nasa.gov/MODLAND_grid.html


- Data extraction in R:
    - run data_processing.R to extract "Monthly daytime 3min CMG Land-surface Temperature" and "Monthly nighttime 3min CMG Land-surface Temperature"
    - Convert the number of day in a year to dates: https://www.epochconverter.com/days/2010
    - See the hdf layer descriptions from: https://icess.eri.ucsb.edu/modis/LstUsrGuide/usrguide_month_cmg.html
    - Local attributes can be found in Table 4: https://icess.eri.ucsb.edu/modis/LstUsrGuide/usrguide_mod11.html#Table_4
    - Spatial unit: 0.05 degree latitude/longitude grids
    - value unit: Kelvin
    - The effective calibration formula for the "LST" SDS is LST = the SDS data in uint16 * 0.02, giving a value in the range of 150-1310.7K.
    - Note: No need to multiply scale_factor in R, didn't find document to prove that, got the conclusion after comparing python output with R output.


- Parallel computing using R
    - Install mpi on mac: https://wiki.helsinki.fi/display/HUGG/Open+MPI+install+on+Mac+OS+X
https://github.com/firemodels/fds/wiki/Installing-Open-MPI-on-OS-X
    - Install RMPI: http://www.stats.uwo.ca/faculty/yu/Rmpi/install.htm, have no permission to install rmpi on ‘/sciclone/aiddata10/REU/R_libs’
Process sample: http://www.glennklockwood.com/data-intensive/r/on-hpc.html

