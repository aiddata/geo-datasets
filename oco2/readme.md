# OCO-2 Carbon Dioxide

Data product: OCO-2 LITE Level 2 version 10r


Product page, documentation, and user guide:
https://disc.gsfc.nasa.gov/datasets/OCO2_L2_Lite_FP_10r/summary

Download base:
https://oco2.gesdisc.eosdis.nasa.gov/data/OCO2_DATA/OCO2_L2_Lite_FP.10r/



# Steps:

1. Link the NASA GESDISC DATA ARCHIVE to your EarthData account as described [here](https://disc.gsfc.nasa.gov/earthdata-login)
    - Login to https://urs.earthdata.nasa.gov/.  If you do not have account then you need to register first.
    - Go to the "Applications" tab and select "Authorized Apps"
    - Click on the "APPROVE MORE APPLICATIONS" button near the bottom of the page
    - Find the "NASA GESDISC DATA ARCHIVE" by scrolling or search and click on the "AUTHORIZE" button for it
    - Agree to the terms and conditions

2. Add your EarthData username and password to the `~/.netrc` file
    - Add `machine urs.earthdata.nasa.gov login <uid> password <password>` to ~/.netrc, where uid and password are your EarthData username and password
    - Do not share your username and password with others

3. Create Conda environment
	- First make sure Anaconda and MPI implementation are available. If on W&M HPC's Vortex nodes for example:
		```
		module load anaconda3/2020.02
		module load openmpi/3.1.4/gcc-9.3.0
		```
	- To create a new environment:
		```
		conda env create -f environment.yml
		conda activate oco2
		pip install mpi4py
        pip install rasterio
		```
    - Note: you may need to install rasterio manually using pip as Conda seems to have an issue with it sometimes
        - rasterio may also appear to install correctly via Conda (either specified under conda packages or pip packages, but fail when actually used
        - To install manually: `pip install rasterio`)

4. If running on W&M HPC, edit jobscript
    - Adjust the resources for the job based on what you would like to request from HPC
    - Edit the `src_dir` variable to the appropriate path for your environment
    - Comment out relevant `mpirun` commands for downloading, processing, or aggregating data based on what you intend to run (see following steps)
	- **Note: If not running on W&M's HPC, please examine the jobscript files for additional environmental configurations. Modifications may be neccesary for running in different environments beyond what is covered in this readme.**


5. Edit the `year_list`, `mode`, `max_workers`, and input/output directory variables in data_download.py and data_prepare.py
    - `year_list`: earliest complete year is 2015 (int or str)
    - `mode` can be either "parallel" or "serial"
    - `max_workers` is the maximum number of processes to use when running in parallel mode. Set this based on the resources you request in your jobscript or what is available in your environment.
    - Note: data_prepare.py has other variables you can edit to alter what stages of data preparation are run, and how data is processed. This is beyond the scope of the readme for reproducing existing data products, but the code is readable and fairly self-explanatory.


6. Run data_download.py,  and data_prepare.py
    - For each stage, comment out unused `mpirun` commands
    - **Note: You may chose to leave all `mpirun` commands uncommented and run the entire pipeline at once.**
    - After the jobscript is edited, start the job:
        - `qsub jobscript`

7. CSV files with the results from each stage can be found in the `raw_dir` variable path







