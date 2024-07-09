# CRU TS Data Ingest

CRU TS (Climatic Research Unit gridded Time Series) is a dataset of monthly climate anomalies on a 0.5x0.5° grid covering the entire world except Antarctica.
It spans the years 1901-2021, and is updated annually.
These scripts download and unzip CRU TS data, convert each month's data from NetCDF to COG, and create yearly aggregates.

## Info

Current version:
CRU TS v. 4.06

Link to data source:
https://crudata.uea.ac.uk/cru/data/hrg/

## Instructions

*The previous versions of code for processing this dataset can be found the archive folder.
These instructions are for running the current Python 3 script using the `Dataset` class.*

1. Download and unzip the CRU TS data

2. [Install conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html), and the conda environment, this should be the same as other datasets that use the shared Dataset class.
   ```sh
   # env.yml is in root of repository
   cd geo-datasets
   conda env create -f env.yml
   conda activate geodata38
   # config.ini and main.py, for example, are in cru_ts
   cd cru_ts
   ```
   Alternatively, you can install the appropriate Python 3 version and packages yourself.

3. Make sure config.ini lists the version of CRU TS that you desire. If it doesn't, see below for instructions on updating the version

4. Set the source/destination folders and the range of years you'd like to process in `config.ini`. Note that all years will be downloaded regardless of how many you'd like to process.

5. Run `main.py`
   ```
   python main.py
   ```

## Downloading new versions

The download links for different versions are a bit complex, so you may have to revise the code to make the new version work properly. In `main.py`, look in the `CRU_TS.download()` function, and adjust the download link formation to point to the latest downloads.


## Reference

Harris, I., Osborn, T.J., Jones, P. et al. Version 4 of the CRU TS monthly high-resolution gridded multivariate climate dataset. Sci Data 7, 109 (2020). https://doi.org/10.1038/s41597-020-0453-3
