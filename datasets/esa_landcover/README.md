# ESA Land Cover

This dataset describes global land use since 1992 using 22 categories. It is [available Copernicus Climate Data Store dataset page](https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-land-cover), and you can explore the data [here](http://maps.elie.ucl.ac.be/CCI/viewer/download.php).

![Example map of land cover from dataset homepage](https://datastore.copernicus-climate.eu/c3s/published-forms-v2/c3sprod/satellite-land-cover/overview.png)

## Quick start:

1. [Create an account for (or login to) the Copernicus Climate Data Store](https://cds.climate.copernicus.eu/user/login?destination=%2Fcdsapp%23!%2Fdataset%2Fsatellite-land-cover%3Ftab%3Doverview)

2. Visit each of the below pages while logged in and agree to the terms and conditions:
    - [ESA CCI licence](https://cds.climate.copernicus.eu/cdsapp/#!/terms/satellite-land-cover)
    - [VITO licence](https://cds.climate.copernicus.eu/cdsapp/#!/terms/vito-proba-v)
    - [Licence to use Copernicus Products](https://cds.climate.copernicus.eu/cdsapp/#!/terms/licence-to-use-copernicus-products)

3. Visit the [CDS API how-to page](https://cds.climate.copernicus.eu/api-how-to) while logged in and follow the instructions to "Install the CDS API Key"
    - Do not follow any of the other steps on that page yet
    - **If the black box only shows `url: {api-url}` and `key: {uid}:{api-key}` you may need to refresh your browser until it populates with your actual API key and information**
    - **If you are on Windows or Mac, follow the appropriate link for instructions, but this readme has only been tested using Linux.**
    - [cdsapi GitHub repo](https://github.com/ecmwf/cdsapi)

4. Create Conda environment
	- First make sure [Conda](https://docs.conda.io/en/latest/) (and optionally MPI) is available. If on W&M HPC's Vortex nodes for example:
		```sh
		module load anaconda3/2021.05
		# if you will be using the MPI backend:
		module load openmpi/3.1.4/gcc-9.3.0
		```
	- To create a new environment:
		```sh
		conda env create -f environment.yml
		conda activate esa_lc
		# if you will be using MPI:
		pip install mpi4py
        pip install cdsapi
		```


6. Edit the variables in `config.ini`
    - `raw_dir` is the directory where the raw data will be downloaded and unzipped in
    - `output_dir` (prepare.py only) is the directory where the final data will be saved
    - `overwrite`, if True, will overwrite existing files rather than skip them
    - `years` is a comma-separated list of years to be processed

7. Run `main.py`
   ```sh
   python main.py
   ```
   **API requests for downloads can take a while to work through queue, run, and be completed before download actually begins.**

## Deploying to Prefect Cloud

1. Log in to Prefect Cloud

2. Run `deploy.py`
   ```sh
   python esa_landcover/deploy.py
   ```

3. From the Deployments menu in Prefect Cloud, select which parameters you'd like to use, then submit the run

## Important Notes

- **At the time of writing, years 2016 and on use an updated "v2.1.1" version. When you run this script, check the dataset information page in case another version applies to a year you are processing.**

- This script aggregates landcover categories to higher levels (combines categories) for easier interpretation using GeoQuery. This category mapping can be adjusted using the `mapping` variable in the `__init__` function of the `ESALandcover` class in `main.py`.

- If you do not want to use any mapping, you may still need to adapt this script to convert the raw data from NetCDF to GeoTIFF (depending on your applications and what tools you plan to use).

- It takes 10-20 minutes to process a single year of data
    - This can vary based on your hardware, but if you are able to full parallelize it should not take too much longer than this to prepare all the data
    - Network speeds between compute and storage could slow this down
    - This is for prepare.py only and does not factor in download times which can vary drastially based on your internet connection
