# MODIS/Terra land surface temperature and emissivity monthly products

Produces monthly and annual land surface temperature products for day and night readings.

Desciption of products:
https://lpdaac.usgs.gov/products/mod11c3v006/

Downloaded from:
https://e4ftl01.cr.usgs.gov/MOLT/MOD11C3.006

Note: MOD11C3.061 is newer processing but not all data is available yet


## Steps:

1. Create an account for (https://urs.earthdata.nasa.gov/)[https://urs.earthdata.nasa.gov/]

2. Create Conda environment
	- First make sure Anaconda and MPI implementation are available. If on W&M HPC's Vortex nodes for example:
		```
		module load anaconda3/2020.02
		module load openmpi/3.1.4/gcc-9.3.0
		```
	- To create a new environment:
		```
		conda env create -f environment.yml
		conda activate geodata
		pip install mpi4py
		```
	- To update your environment (if needed):
		```
		conda env update --prefix ./env --file environment.yml  --prune
	- To export your environment (if needed):
		```
		conda env export > environment.yml
		```
    - **Note: If you export you environment, make sure to remove mpi4py from the environment.yml file. Conda cannot properly install mpi4py, it must be installed using pip (mpi4py needs to be built using the MPI install on the system, at least on W&M's HPC)**


3. If running on W&M HPC, edit jobscript
    - Adjust the resources for the job based on what you would like to request from HPC
    - Edit the `src_dir` variable to the appropriate path for your environment
    - Comment out relevant `mpirun` commands for downloading, processing, or aggregating data based on what you intend to run (see following steps)
	- **Note: If not running on W&M's HPC, please examine the jobscript files for additional environmental configurations. Modifications may be neccesary for running in different environments beyond what is covered in this readme.**

4. Add your Earthdata username and password to the data_download.py file
    - Do not share your user credentials with anyone else.

5. Edit the `mode` and `max_workers` variables in data_download.py, data_processing.py, and data_aggregation.py
    - `mode` can be either "parallel" or "serial"
    - `max_workers` is the maximum number of processes to use when running in parallel mode. Set this based on the resources you request in your jobscript or what is available in your environment.

6. Edit the paths for your data in each file:
    - The `output_dir` variable in data_download.py
    - The `input_dir`and `output_dir` variables in data_processing.py
    - The `input_dir` variable in data_aggregation.py
    - **Note: `output_dir` from data_download.py must match `input_dir` from data_processing.py**
    - **Note: `output_dir` from data_processing.py must match `input_dir` from data_aggregation.py**

6. Run data download, data processing, and data aggregation
    - For each stage, comment out unused `mpirun` commands
    - **Note: You may chose to leave all 3 `mpirun` commands uncommented and run the entire pipeline at once.**
    - After the jobscript is edited, start the job:
        - `qsub jobscript`

7. CSV files with the results from each stage can be found in:
    - The `output_dir` variable in data_download.py
    - The `input_dir` variable in data_processing.py
    - The `input_dir` variable in data_aggregation.py


