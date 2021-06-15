# CRU TS Data Ingest

CRU TS (Climatic Research Unit gridded Time Series) is a dataset of monthly climate anomalies on a 0.5x0.5° grid covering the entire world except Antarctica.
It spans the years 1901-2020, and is updated annually.
These scripts download and unzip CRU TS data (download.sh), convert each month's data from NetCDF to GeoTIFF, and create yearly aggregates. (extract_data.py)

## Version Info

Current version:
CRU TS v. 4.05

Link to data source:
https://crudata.uea.ac.uk/cru/data/hrg/

## Instructions

*n.b. At the time of writing, the Python 3 script is more up-to-date than the R script.
These instructions are for running the Python 3 script.*

1. Download and unzip the CRU TS data
   ```
   bash download.sh
   ```

3. [Install conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html), and the conda environment environment.yml
   ```
   bash create_env.sh
   ```
   Conda will confirm that the environment was created, and give you the command to activate it:
   ```
   conda activate geodata-cru
   ```
   Alternatively, you can install the appropriate Python 3 version and packages yourself.
   
4. Make sure your copy of the CRU TS data matches the version listed at the top of extract_data.py
   If your copy is a newer version, all scripts will need to be udpated to support it.
   
5. Set the source/destination folders and the range of years you'd like to process near the top of extract_data.py (see comments)

6. Run extract_data.py
   ```
   python extract_data.py
   ```

## Reference

Harris, I., Osborn, T.J., Jones, P. et al. Version 4 of the CRU TS monthly high-resolution gridded multivariate climate dataset. Sci Data 7, 109 (2020). https://doi.org/10.1038/s41597-020-0453-3
