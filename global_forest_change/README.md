# Global Forest Change

Current product:
- Version 1.8 - Global Forest Change 2000–2020


Product page and documentation:
https://storage.googleapis.com/earthenginepartners-hansen/GFC-2020-v1.8/download.html

Citation:
```
Hansen, M. C., P. V. Potapov, R. Moore, M. Hancher, S. A. Turubanova, A. Tyukavina, D. Thau, S. V. Stehman, S. J. Goetz, T. R. Loveland, A. Kommareddy, A. Egorov, L. Chini, C. O. Justice, and J. R. G. Townshend. 2013. “High-Resolution Global Maps of 21st-Century Forest Cover Change.” Science 342 (15 November): 850–53. Data available on-line from: http://earthenginepartners.appspot.com/science-2013-global-forest.
```


# Steps:

1. Create Conda environment
	- First make sure Anaconda and MPI implementation are available. If on W&M HPC's Vortex nodes for example:
		```
		module load anaconda3/2020.02
		module load openmpi/3.1.4/gcc-9.3.0
		```
	- To create a new environment:
		```
		conda env create -f environment.yml
		conda activate gfc
		pip install mpi4py
		```
	- You may need to install rasterio using pip (`pip install rasterio`) as Conda can have some dependency issues when installing rasterio.

2. If running on W&M HPC, edit jobscript
    - Adjust the resources for the job based on what you would like to request from HPC
    - Edit the `src_dir` variable to the appropriate path for your environment
    - Comment out relevant `mpirun` commands for downloading or preparing data based on what you intend to run (see following steps)
	- **Note: If not running on W&M's HPC, please examine the jobscript files for additional environmental configurations. Modifications may be neccesary for running in different environments beyond what is covered in this readme.**


3. Edit the  `mode` and `max_workers`, and input/output directory variables in data_download.py and data_prepare.py
    - `mode` can be either "parallel" or "serial"
    - `max_workers` is the maximum number of processes to use when running in parallel mode. Set this based on the resources you request in your jobscript or what is available in your environment.
    - You can also adjust the version if updating for future versions of the global forest change data (if they change their data access patterns other parts of scripts may fail)


4. Run data_download.py and data_prepare.py
    - For each stage, comment out unused `mpirun` commands
    - **Note: You may chose to leave all `mpirun` commands uncommented and run the entire pipeline at once.**
    - After the jobscript is edited, start the job:
        - `qsub jobscript`

5. CSV files with the results from each stage can be found in the `results` dir within path specified by the `raw_dir` variable in data_download.py and or the `input_dir` variable in data_prepare.py (**Note: these 2 paths should be the same**).

