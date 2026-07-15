import json
from pathlib import Path
from typing import List, Optional

import geopandas as gpd
import requests
from pydantic import Field, field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config


class geoBoundariesDownloadConfiguration(BaseDatasetConfiguration):
    version: str
    gb_web_hash: str
    output_dir: str
    skip_existing: bool
    dl_iso3_list: List[str] = []

    @field_validator("output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class geoBoundariesDownloadDataset(Dataset):

    name = "geoBoundariesDownload"

    def __init__(self, config: geoBoundariesDownloadConfiguration):
        self.config = config
        self.output_tag = f"gB{config.version}"
        self.output_path = config.output_dir / config.gb_web_hash
        self.skip_existing = config.skip_existing
        self.dl_iso3_list = config.dl_iso3_list or []
        self.api_url = f"https://raw.githubusercontent.com/wmgeolab/gbWeb/{config.gb_web_hash}/api/current/gbOpen/ALL/ALL/index.json"

    def prepare(self):
        logger = self.get_logger()

        logger.info(f"Fetching geoBoundaries metadata from {self.api_url}")
        response = requests.get(self.api_url)
        response.raise_for_status()
        api_data = response.json()

        if self.dl_iso3_list:
            dl_items = [i for i in api_data if i["boundaryISO"] in self.dl_iso3_list]
        else:
            dl_items = list(api_data)

        dl_items = sorted(dl_items, key=lambda d: d["boundaryISO"])
        logger.info(f"Found {len(dl_items)} items to download")

        return [(i,) for i in dl_items]

    def dl_gb_item(self, item: dict):
        logger = self.get_logger()

        iso3 = item["boundaryISO"]
        fc_type = item["boundaryType"]
        fc_name = f"{self.output_tag}_{iso3}_{fc_type}"

        logger.info(f"Processing: {fc_name}")

        dl_url = item["gjDownloadURL"]
        gpkg_path = self.output_path / f"{Path(dl_url).stem}.gpkg"
        raw_meta_path = self.output_path / f"raw_{Path(dl_url).stem}.json"

        if self.skip_existing and gpkg_path.exists() and raw_meta_path.exists():
            logger.warning(f"Skipping existing: {fc_name}")
            return

        logger.debug(f"Downloading {dl_url}")
        try:
            gdf = gpd.read_file(dl_url)
        except Exception:
            if requests.get(dl_url).status_code == 404:
                logger.error(f"404: {dl_url}")
                return
            else:
                try:
                    raw_json = requests.get(dl_url).json()
                    gdf = gpd.GeoDataFrame.from_features(raw_json["features"])
                except Exception as e:
                    logger.error(f"Failed to download {dl_url}: {e}")
                    return

        if "shapeName" not in gdf.columns:
            potential_name_field = f"{fc_type}_NAME"
            if potential_name_field in gdf.columns:
                gdf["shapeName"] = gdf[potential_name_field]
            else:
                gdf["shapeName"] = None

        gdf.to_file(gpkg_path, driver="GPKG")

        with open(raw_meta_path, "w") as file:
            json.dump(item, file, indent=4)

        logger.info(f"Successfully downloaded {fc_name}")

    def main(self):
        logger = self.get_logger()

        self.output_path.mkdir(exist_ok=True, parents=True)

        ingest_items = self.prepare()

        logger.info("Running geoBoundaries download")
        dl_run = self.run_tasks(self.dl_gb_item, ingest_items)
        self.log_run(dl_run)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def geoBoundariesDownloadFlow(config: geoBoundariesDownloadConfiguration):
        geoBoundariesDownloadDataset(config).run(config.run)


if __name__ == "__main__":
    config = get_config(geoBoundariesDownloadConfiguration)
    geoBoundariesDownloadDataset(config).run(config.run)
