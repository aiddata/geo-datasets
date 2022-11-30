conda create -n pm25 python=3.8 affine bokeh dask-jobqueue netcdf4 numpy proj rasterio
conda activate pm25
conda install -c conda-forge prefect prefect-dask
pip install "boxsdk[jwt]" mpi4py pandas
