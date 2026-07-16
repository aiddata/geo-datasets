"""
US Census Bureau TIGER/Line Shapefiles

https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

Downloads a national TIGER/Line layer (e.g. COUNTY, STATE) for a given year,
processes it to a GeoPackage, and writes an ingest JSON. Runs a single layer per
flow run; per-state layers (tract, block group) are out of scope.

    County: https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip
"""
import json
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config


class TIGERDownloadConfiguration(BaseDatasetConfiguration):
    year: str
    dataset: str
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_process: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class TIGERDownloadDataset(Dataset):

    name = "TIGERDownload"

    def __init__(self, config: TIGERDownloadConfiguration):
        self.config = config
        self.year = config.year
        self.dataset = config.dataset
        self.overwrite_download = config.overwrite_download
        self.overwrite_process = config.overwrite_process

        stem = f"tl_{self.year}_us_{self.dataset.lower()}"
        self.url = f"https://www2.census.gov/geo/tiger/TIGER{self.year}/{self.dataset}/{stem}.zip"
        self.download_path = config.raw_dir / "TIGER" / f"{stem}.zip"
        self.output_path = config.output_dir / "TIGER" / f"{stem}.gpkg"

    def download(self):
        logger = self.get_logger()

        if not self.overwrite_download and self.download_path.exists():
            logger.info(f"File {self.download_path} already exists. Skipping download.")
            return

        logger.info(f"Downloading {self.url} to {self.download_path}")
        response = requests.get(self.url)
        response.raise_for_status()

        with open(self.download_path, "wb") as f:
            f.write(response.content)

        logger.info(f"Downloaded {self.download_path}")

    def process(self):
        logger = self.get_logger()

        if not self.overwrite_process and self.output_path.exists():
            logger.info(f"GeoPackage {self.output_path} already exists. Skipping processing.")
            return

        gdf = gpd.read_file(self.download_path)
        logger.info(f"Loaded {len(gdf)} features from {self.download_path}")

        assert gdf.is_valid.all(), "Some geometries are invalid. Consider filtering them out."

        # fill nulls per column based on the dtype it should hold without nulls
        for column in gdf.columns:
            if column == "geometry":
                continue
            elif pd.api.types.is_numeric_dtype(gdf[column]):
                gdf[column] = gdf[column].fillna(0)
            else:
                gdf[column] = gdf[column].fillna("")

        gdf.to_file(self.output_path, driver="GPKG")
        logger.info(f"Saved to {self.output_path}")

    def create_ingest_json(self):
        logger = self.get_logger()

        defaults = {
            "name": f"TIGER_{self.year}_{self.dataset}",
            "short_name": f"TIGER_{self.year}_{self.dataset}",
            "file_mask": "None",
            "active": 1,
            "public": 1,
            "path": str(Path("/data/datasets/TIGER") / self.output_path.name),
            "file_extension": ".gpkg",
            "title": f"US CENSUS TIGER/Line {self.year} {self.dataset}",
            "description": f"US CENSUS TIGER/Line shapefiles for {self.dataset} in {self.year}",
            "details": "",
            "tags": ["TIGER", "Census", self.dataset, f"{self.year}", "USA"],
            "citation": f"U.S. Census Bureau. ({self.year}). {self.year} TIGER/Line Shapefiles: {self.dataset} (machine readable data files). U.S. Department of Commerce. census.gov (Accessed {datetime.now().strftime('%Y-%m-%d')}).",
            "source_name": "United States Census Bureau",
            "source_url": "https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html",
            "other": {},
            "ingest_src": "geoquery_automated",
            "is_global": False,
            "spatial_extent": None,
            "group_name": "TIGER",
            "group_title": "US TIGER/Line Shapefiles",
            "group_class": "parent",
            "group_level": "2",
        }

        ingest_json_path = (
            self.output_path.parent / f"ingest_{self.year}_{self.dataset.lower()}.json"
        )
        with open(ingest_json_path, "w") as f:
            json.dump(defaults, f, indent=4)

        logger.info(f"Ingest JSON created at {ingest_json_path}")

    def main(self):
        logger = self.get_logger()

        self.download_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Running TIGER download for {self.year} {self.dataset}")
        self.download()
        self.process()
        self.create_ingest_json()


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def TIGERDownloadFlow(config: TIGERDownloadConfiguration):
        TIGERDownloadDataset(config).run(config.run)


if __name__ == "__main__":
    config = get_config(TIGERDownloadConfiguration)
    TIGERDownloadDataset(config).run(config.run)
