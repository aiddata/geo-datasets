"""
Africa Child Mortality — Under-5 mortality rate by decade

https://www.thelancet.com/journals/langlo/article/PIIS2214-109X(16)30240-3

Point estimates of under-5 mortality (from Burke, Heft-Neal & Bendavid, 2016)
downloaded as a single space-separated text file, then rasterized per decade
(1980, 1990, 2000) using the distancerasters library.
"""
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from distancerasters import rasterize
from pydantic import field_validator
from shapely.geometry import Point

from data_manager import BaseDatasetConfiguration, Dataset, get_config

DOWNLOAD_URL = (
    "https://www.dropbox.com/s/jfymdx15ustdlwr/"
    "ChildMortality5m0Estimates_Burke-HeftNeal-Bendavid_v1.txt?dl=1"
)
PIXEL_SIZE = 0.1
DATA_FIELD = "est5m0"
DECADES = [1980, 1990, 2000]


class AfricaChildMortalityConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_process: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class AfricaChildMortality(Dataset):

    name = "Africa Child Mortality"

    def __init__(self, config: AfricaChildMortalityConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_download = config.overwrite_download
        self.overwrite_process = config.overwrite_process

        self.download_path = (
            self.raw_dir / "ChildMortality5m0Estimates_Burke-HeftNeal-Bendavid_v1.txt"
        )

    def download(self):
        logger = self.get_logger()

        if not self.overwrite_download and self.download_path.exists():
            logger.info(f"Download exists, skipping: {self.download_path}")
            return

        logger.info(f"Downloading {DOWNLOAD_URL} to {self.download_path}")
        with self.tmp_to_dst_file(self.download_path, make_dst_dir=True) as tmp:
            response = requests.get(DOWNLOAD_URL, timeout=60)
            response.raise_for_status()
            with open(tmp, "wb") as dst:
                dst.write(response.content)
        logger.info(f"Downloaded {self.download_path}")

    def process(self, decade):
        logger = self.get_logger()

        output_path = self.output_dir / f"africa_child_mortality_{decade}.tif"

        if not self.overwrite_process and output_path.exists():
            logger.info(f"Output exists, skipping: {output_path}")
            return

        df = pd.read_table(self.download_path, sep=" ")
        df = df[df["decade"] == decade]
        df["geometry"] = df.apply(lambda z: Point(z["lon"], z["lat"]), axis=1)
        gdf = gpd.GeoDataFrame(df)

        logger.info(f"Rasterizing decade {decade} to {output_path}")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        rasterize(
            gdf,
            attribute=DATA_FIELD,
            pixel_size=PIXEL_SIZE,
            bounds=gdf.geometry.total_bounds,
            output=str(output_path),
            fill=-1,
            nodata=-1,
        )
        logger.info(f"Wrote {output_path}")

    def main(self):
        logger = self.get_logger()

        logger.info("Running Africa child mortality download")
        self.download()

        logger.info("Running rasterization")
        process = self.run_tasks(self.process, [[decade] for decade in DECADES])
        self.log_run(process)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def africa_child_mortality(config: AfricaChildMortalityConfiguration):
        AfricaChildMortality(config).run(config.run)


if __name__ == "__main__":
    config = get_config(AfricaChildMortalityConfiguration)
    AfricaChildMortality(config).run(config.run)
