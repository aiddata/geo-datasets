conda create -n pm25 python=3.9
conda activate pm25
conda install affine netcdf4 numpy pandas proj
pip install mpi4py prefect rasterio "boxsdk[jwt]" prefect-dask dask-jobqueue
