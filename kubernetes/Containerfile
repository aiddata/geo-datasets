FROM prefecthq/prefect:2.7.3-python3.8

# install gdal
RUN apt-get update
RUN apt-get install -y gdal-bin libgdal-dev

# install python packages
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-binary rasterio

# run prefect agent
CMD ["prefect", "agent", "start", "-q", "geodata"]
