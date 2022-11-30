# LTDR NDVI Data Ingest

[LTDR (Long-Term Data Record)](https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/applications/ltdr/) is a project at NASA that "produces, validates and distributes a climate data record." [NDVI (Normalized Difference Vegetation Index)](https://modis-land.gsfc.nasa.gov/vi.html) "provides continuity with NOAA's AVHRR NDVI time series record for historical and climate applications."
These scripts download daily NDVI data, unpacks them from HDF containers into the GeoTIFF format, and create monthly and yearly aggregates.

## Instructions

1. Create EarthData login for LAADS
https://urs.earthdata.nasa.gov/oauth/authorize?response_type=code&client_id=A6th7HB-3EBoO7iOCiCLlA&redirect_uri=https://ladsweb.modaps.eosdis.nasa.gov/login&state=/tools-and-services/data-download-scripts/

2. Generate an app key:
https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/data-download-scripts/

3. Add app key to download script ("PUT YOUR APP KEY HERE" in download.sh)

4. Run download.sh to download data set
   ```
   bash download.sh
   ```
5. [Install conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html), and create the conda environment defined by environment.yml (Note: if you do not plan on running prepare_daily.py in parallel using mpi4py, comment out the `pip install mpi4py` line of `create_env.sh`)
   ```
   bash create_env.sh
   ```
   Conda will confirm that the environment was created, and give you the command to activate it:
   ```
   conda activate ltdr_ndvi
   ```
   Alternatively, you can install the appropriate Python 3 version and packages yourself.

6. Either run prepare_daily.py serially
   ```
   python prepare_daily.py
   ```
   …or in parallel (review jobscript before submitting!)
   ```
   qsub jobscript
   ```
