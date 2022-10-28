# ESA Land Cover


Copernicus Climate Data Store dataset page:
https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-land-cover


To explore data:
http://maps.elie.ucl.ac.be/CCI/viewer/download.php


## Steps:

1. Create (or login to) for the Copernicus Climate Data Store
    - https://cds.climate.copernicus.eu/user/login?destination=%2Fcdsapp%23!%2Fdataset%2Fsatellite-land-cover%3Ftab%3Doverview

2. Visit each of the below pages while logged in and agree to the terms and conditions:
    - https://cds.climate.copernicus.eu/cdsapp/#!/terms/satellite-land-cover
    - https://cds.climate.copernicus.eu/cdsapp/#!/terms/vito-proba-v
    - https://cds.climate.copernicus.eu/cdsapp/#!/terms/licence-to-use-copernicus-products

3. Visit the following page while logged in and follow the instructions to "Install the CDS API Key"
    - https://cds.climate.copernicus.eu/api-how-to
    - Do not follow any of the other steps on that page yet
    - **Note: If the black box only shows `url: {api-url}` and `key: {uid}:{api-key}` you may need to refresh your browser until it populates with your actual API key and information**
    - **Note: If you are on Windows or Mac, follow the appropriate link for instructions, but this readme has only been tested using Linux.**
    - [cdsapi GitHub repo](https://github.com/ecmwf/cdsapi)

4. Create Conda environment
	- First make sure Anaconda and MPI implementation are available. If on W&M HPC's Vortex nodes for example:
		```
		module load anaconda3/2021.05
		module load openmpi/3.1.4/gcc-9.3.0
		```
	- To create a new environment:
		```
		conda env create -f environment.yml
		conda activate esa_lc
		pip install mpi4py
        pip install cdsapi
		```

5. If running on W&M HPC, edit jobscript
    - Adjust the resources for the job based on what you would like to request from HPC
    - Edit the `src_dir` variable to the appropriate path for your environment
    - Comment out relevant `mpirun` commands for downloading or preparing based on what you intend to run (see following steps)
	- **Note: If not running on W&M's HPC, please examine the jobscript files for additional environmental configurations. Modifications may be neccesary for running in different environments beyond what is covered in this readme.**


6. Edit the variables in download.py and prepare.py
    - `raw_dir` is the directory where the raw data will be downloaded and unzipped in
    - `output_dir` (prepare.py only) is the directory where the final data will be saved
    - `years` is a list of int or str for years to be processed
    - `mode` can be either "parallel" or "serial"
    - `max_workers` is the maximum number of processes to use when running in parallel mode. Set this based on the resources you request in your jobscript or what is available in your environment.
    - `v211_years` (download.py only) is the range of years produced under version 2.1.1
        - **Note: Review download patterns used by ESA in future before extending v211_years variable past 2019. Other changes in script may be needed if patterns change.**

7. Run download.py and/or prepare.py using jobscript or calling directly
    - e.g., `qsub jobscript` or `python download.py`
    - **Note: API requests for downloads can take a while to work through queue, run, and be completed before download actually begins.**
    - `prepare.py` aggregates landcover categories to higher levels (combines categories) for easier interpretation using GeoQuery. This category mapping can be adjusted using the `mapping` variable in `prepare.py`.
        - Note: If you do not want to use any mapping, you may still need to adapt this script to convert the raw data from NetCDF to GeoTIFF (depending on your applications and what tools you plan to use).


### Additional notes:
- `prepare.py` takes 10-20 minutes to run for a single year of data
    - This can vary based on your hardware, but if you are able to full parallelize it should not take too much longer than this to prepare all the data
    - Network speeds between compute and storage could slow this down
    - This is for prepare.py only and does not factor in download times which can vary drastially based on your internet connection
