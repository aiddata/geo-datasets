
from pathlib import Path
from typing import List, Optional
import json
import shapely
import requests
import geopandas as gpd
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config


def get_api_url(url):
    response = requests.get(url)
    content = response.json()
    return content


class geoBoundariesConfiguration(BaseDatasetConfiguration):
    version: str
    gb_data_hash: str
    gb_web_hash: str
    raw_dir: str
    output_dir: str
    overwrite_existing: bool
    dl_iso3_list: Optional[List[str]] = None

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class geoBoundariesDataset(Dataset):

    name = "geoBoundaries"

    def __init__(self, config: geoBoundariesConfiguration):

        self.config = config

        self.raw_dir = config.raw_dir / config.version
        self.output_dir = config.output_dir / f"{config.version}_{config.gb_data_hash}_{config.gb_web_hash}"

        self.overwrite_existing = config.overwrite_existing

        # leave blank / set to None to download all ISO3 boundaries
        self.dl_iso3_list = config.dl_iso3_list


        self.raw_dir.mkdir(exist_ok=True, parents=True)
        self.output_dir.mkdir(exist_ok=True, parents=True)

        self.api_url = f"https://raw.githubusercontent.com/wmgeolab/gbWeb/{config.gb_web_hash}/api/current/gbOpen/ALL/ALL/index.json"

        self.default_meta = {
            "active": 0,
            "public": 0,
            "name": None,
            "path": None,
            "file_extension": ".gpkg",
            "title": None,
            "description": None,
            "details": "",
            "tags": ["geoboundaries", "administrative", "boundary"],
            "citation": "Runfola, D. et al. (2020) geoBoundaries: A global database of political administrative boundaries. PLoS ONE 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866",
            "source_name": "geoBoundaries",
            "source_url": "geoboundaries.org",
            "other": {},
            "ingest_src": "geoquery_automated",
            "is_global": False,
            "group_name": None,
            "group_title": None,
            "group_class": None,
            "group_level": None,
            "features": None
        }


    def prepare(self):
        """
        Prepare data for download
        """
        logger = self.get_logger()

        api_data = get_api_url(self.api_url)

        if self.dl_iso3_list:
            ingest_items = [(i,) for i in api_data if i["boundaryISO"] in self.dl_iso3_list]
        else:
            ingest_items = [(i,) for i in api_data]

        ingest_items = sorted(ingest_items, key=lambda d: d[0]['boundaryISO'])
        return ingest_items


    def dl_gb_item(self, item: dict):
        """
        Download and process a single geoBoundaries item
        """
        logger = self.get_logger()

        iso3 = item["boundaryISO"]

        adm_meta = self.default_meta.copy()

        adm_meta["name"] = f"gB_v6_{iso3}_{item['boundaryType']}"

        logger.info(f"Processing geoBoundaries item: {adm_meta['name']}")

        adm_meta[
            "title"
        ] = f"geoBoundaries v6 - {item['boundaryName']} {item['boundaryType']}"
        adm_meta[
            "description"
        ] = f"This feature collection represents the {item['boundaryType']} level boundaries for {item['boundaryName']} ({iso3}) from geoBoundaries v6."
        adm_meta["details"] = ""
        adm_meta["group_name"] = f"gb_v6_{iso3}"
        adm_meta["group_title"] = f"gB v6 - {iso3}"
        adm_meta["group_class"] = (
            "parent" if item["boundaryType"] == "ADM0" else "child"
        )
        adm_meta["group_level"] = int(item["boundaryType"][3:])

        # save full metadata from geoboundaries api to the "other" field
        adm_meta["other"] = item.copy()

        commit_dl_url = item["gjDownloadURL"]
        # "https://github.com/wmgeolab/geoBoundaries/raw/c0ed7b8/releaseData/gbOpen/AFG/ADM0/geoBoundaries-AFG-ADM0.geojson",

        # commit_dl_url = raw_dl_url.replace(raw_dl_url.split("/")[6], target_gb_commit)

        gpkg_path = self.output_dir / f"{Path(commit_dl_url).stem}.gpkg"
        adm_meta["path"] = str(gpkg_path)

        geojson_path = gpkg_path.with_suffix(".geojson")
        json_path = gpkg_path.with_suffix(".test.json")

        if gpkg_path.exists() and geojson_path.exists() and json_path.exists() and not self.overwrite_existing:
            logger.info(f"Skipping existing file: {gpkg_path}")
            return

        logger.debug(f"Downloading {commit_dl_url}")
        try:
            gdf = gpd.read_file(commit_dl_url)
        except:
            if requests.get(commit_dl_url).status_code == 404:
                logger.error(f"404: {commit_dl_url}")
                return
            else:
                try:
                    raw_json = get_api_url(commit_dl_url)
                    gdf = gpd.GeoDataFrame.from_features(raw_json["features"])
                except:
                    logger.error(f"Failed to download {commit_dl_url}")
                    return


        if "shapeName" not in gdf.columns:
            potential_name_field = f'{item["boundaryType"]}_NAME'
            if potential_name_field in gdf.columns:
                gdf["shapeName"] = gdf[potential_name_field]
            else:
                gdf["shapeName"] = None

        gdf.to_file(gpkg_path, driver="GPKG")
        gdf.to_file(geojson_path, driver="GeoJSON")

        logger.debug(f"Getting bounding box for {commit_dl_url}")
        spatial_extent = shapely.box(*gdf.total_bounds).wkt
        adm_meta["spatial_extent"] = spatial_extent


        # export to json
        export_adm_meta = adm_meta.copy()
        with open(json_path, "w") as file:
            json.dump(export_adm_meta, file, indent=4)


    def main(self):

        logger = self.get_logger()

        ingest_items = self.prepare()

        # logger.info("Running data download")
        # dl_run = self.run_tasks(self.dl_gb_item, ingest_items)
        # self.log_run(dl_run)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def geoBoundariesFlow(config: geoBoundariesConfiguration):
        geoBoundariesDataset(config).run(config.run)


if __name__ == "__main__":
    config = get_config(geoBoundariesConfiguration)
    geoBoundariesDataset(config).run(config.run)
