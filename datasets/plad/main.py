# data download script for PLAD political leaders' birthplace dataset
# info link: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YUS575

import os
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
import rasterio
import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from rasterio import features
from shapely.geometry import Point


class PLADConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    years: List[int]
    max_retries: int
    overwrite_download: bool
    overwrite_output: bool


class PLAD(Dataset):

    name = "PLAD"

    def __init__(self, config: PLADConfiguration):

        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = config.years
        self.max_retries = config.max_retries
        self.overwrite_download = config.overwrite_download
        self.overwrite_output = config.overwrite_output
        self.dataset_url = "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YUS575"
        self.download_url = "https://dataverse.harvard.edu/api/access/datafile/10119325?format=tab&gbrecs=true"

        self.src_path = self.raw_dir / "plad.tab"

    def test_connection(self):
        # test connection
        test_request = requests.get(self.dataset_url, verify=True)
        test_request.raise_for_status()

    def download_data(self):
        """
        Download original spreadsheet
        """
        logger = self.get_logger()

        if os.path.isfile(self.src_path) and not self.overwrite_download:
            logger.info(f"Download Exists: {self.src_path}")
        else:
            attempts = 1
            while attempts <= self.max_retries:
                try:
                    with requests.get(self.download_url, stream=True, verify=True) as r:
                        r.raise_for_status()
                        with open(self.src_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                    logger.info(f"Downloaded: {self.download_url}")
                    return (self.download_url, self.src_path)
                except Exception as e:
                    attempts += 1
                    if attempts > self.max_retries:
                        logger.info(
                            f"{str(e)}: Failed to download: {str(self.download_url)}"
                        )
                        return (self.download_url, self.src_path)
                    else:
                        logger.info(
                            f"Attempt {str(attempts)} : {str(self.download_url)}"
                        )

    def process_year(self, year):
        """create file for each year"""

        logger = self.get_logger()

        output_filename = f"leader_birthplace_data_{year}.tif"
        output_path = self.output_dir / output_filename

        if os.path.isfile(output_path) and not self.overwrite_output:
            logger.info(f"File exists: {str(output_path)}")
            return ("File exists", str(output_path))

        if not os.path.isfile(self.src_path):
            logger.info(f"Error: Master data download: {self.src_path} not found")
            raise Exception(f"Data file not found: {self.src_path}")

        src_df = pd.read_csv(self.src_path, sep="\t", low_memory=False)

        # adm2 or finer precision
        # valid lat/lon and not foreign leader
        df = src_df.loc[
            (src_df.longitude != ".")
            & (src_df.latitude != ".")
            & (src_df.foreign_leader.isin([0, "0"]))
            & (src_df.geo_precision.isin([1, 2, 3]))
        ].copy()

        df = df.loc[(df.startyear <= year) & (df.endyear >= year)].copy()

        df["geometry"] = df.apply(lambda x: Point(x.longitude, x.latitude), axis=1)

        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        gdf = gdf.set_crs(epsg=4326)

        pixel_size = 0.05
        transform = rasterio.transform.from_origin(-180, 90, pixel_size, pixel_size)
        shape = (int(180 / pixel_size), int(360 / pixel_size))

        rasterized = features.rasterize(
            gdf.geometry,
            out_shape=shape,
            fill=0,
            out=None,
            transform=transform,
            all_touched=True,
            default_value=1,
            dtype=None,
        )

        with rasterio.open(
            output_path,
            "w",
            driver="GTiff",
            crs="EPSG:4326",
            transform=transform,
            dtype=rasterio.uint8,
            count=1,
            width=shape[1],
            height=shape[0],
        ) as dst:
            dst.write(rasterized, indexes=1)

        logger.info(f"Data Compiled: {str(year)}")

    def main(self):

        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        logger.info("Running data download")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.download_data()

        logger.info("Sorting Data")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        sort = self.run_tasks(
            self.process_year,
            [
                [
                    y,
                ]
                for y in self.years
            ],
        )
        self.log_run(sort)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def plad(config: PLADConfiguration):
        PLAD(config).run(config.run)


if __name__ == "__main__":
    config = get_config(PLADConfiguration)
    PLAD(config).run(config.run)
