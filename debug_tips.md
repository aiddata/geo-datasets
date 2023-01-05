* package versions:
    - prefect==2.7.3
    - prefect-dask==0.2.2
    - dask-jobqueue==0.8.1
    - rasterio==1.3.3 (needs to be installed using pip typically)

* make sure TMPDIR variable is set in environment running prefect agent

* paths expected to be Path objects must be defined as Path objected within their class function