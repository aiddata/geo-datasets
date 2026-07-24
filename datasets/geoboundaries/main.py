import json
from pathlib import Path

import geopandas as gpd
import requests
from pydantic import field_validator
import shapely

from data_manager import BaseDatasetConfiguration, Dataset, get_config


class geoBoundariesDownloadConfiguration(BaseDatasetConfiguration):
    version: str
    gb_web_hash: str # commit
    output_dir: str
    skip_existing: bool
    rebuild_meta: bool
    # Comma-separated ISO3 codes (e.g. "AFG,GHA"); empty means download all.
    # Kept as a string rather than a list so the Prefect run form renders a text
    # input instead of the array widget, whose "add item" button submits the form.
    dl_iso3_list: str = ""
    ingest_dir: str = "/data/boundaries/geoboundaries"

    @field_validator("output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class geoBoundariesDownloadDataset(Dataset):

    name = "geoBoundariesDownload"

    def __init__(self, config: geoBoundariesDownloadConfiguration):
        self.config = config
        self.output_tag = f"gB{config.version}"
        self.commit = config.gb_web_hash
        self.output_path = config.output_dir / self.commit
        self.skip_existing = config.skip_existing
        self.rebuild_meta = config.rebuild_meta
        self.dl_iso3_list = [
            code.strip() for code in config.dl_iso3_list.split(",") if code.strip()
        ]
        self.api_url = f"https://raw.githubusercontent.com/wmgeolab/gbWeb/{self.commit}/api/current/gbOpen/ALL/ALL/index.json"
        self.ingest_dir = Path(config.ingest_dir)

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
        fc_name = f"{self.output_tag}_{self.commit}_{iso3}_{fc_type}"

        logger.info(f"Processing: {fc_name}")

        dl_url = item["gjDownloadURL"]
        self.gpkg_name = f"{Path(dl_url).stem}.gpkg"
        gpkg_path = self.output_path / self.gpkg_name
        raw_meta_path = self.output_path / f"raw_{Path(dl_url).stem}.json"

        meta_only = False
        if gpkg_path.exists() and raw_meta_path.exists():
            if self.rebuild_meta:
                logger.info(f"Rebuilding metadata for {fc_name}")
                meta_only = True
        elif self.skip_existing:
            logger.warning(f"Skipping existing: {fc_name}")
            return
        else:
            logger.info(f"Downloading: {dl_url}")

        if meta_only:
            try:
                gdf = gpd.read_file(gpkg_path)
            except Exception as e:
                logger.error(f"Failed to read existing GPKG {gpkg_path}: {e}")
                return
        else:
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


        # Export raw metadata from gB
        with open(raw_meta_path, "w") as file:
            json.dump(item, file, indent=4)

        adm_meta = self.build_metadata(item, fc_name)

        # Calculate spatial extent
        logger.debug(f"Calculating bounding box for {fc_name}")
        spatial_extent_wkt = shapely.box(*gdf.total_bounds).wkt
        adm_meta["spatial_extent"] = spatial_extent_wkt

        # Export processed metadata for GeoQuery ingest
        export_meta = {k: v for k, v in adm_meta.items() if k != "features"}
        export_meta["spatial_extent"] = spatial_extent_wkt
        json_path = gpkg_path.with_suffix(".json")
        with open(json_path, "w") as file:
            json.dump(export_meta, file, indent=4)

        logger.info(f"Successfully downloaded {fc_name}")


    def build_metadata(self, item: dict, fc_name: str) -> dict:
        """Build metadata dictionary for a geoBoundaries item."""
        iso3 = item["boundaryISO"]
        fc_type = item["boundaryType"]

        raw_github_string_prefix = "https://github.com/wmgeolab/geoBoundaries/raw/"
        data_commit = item["gjDownloadURL"].replace(raw_github_string_prefix, "").split("/")[0]

        return {
            "active": False,
            "public": True,
            "name": fc_name,
            "path": str(self.ingest_dir / self.commit / self.gpkg_name), # strip out everything before /data/boundaries
            "file_extension": ".gpkg",
            "title": f"geoBoundaries v6 - {item['boundaryName']} {fc_type}",
            "description": (
                f"This feature collection represents the {fc_type} level "
                f"boundaries for {item['boundaryName']} ({iso3}) from geoBoundaries v6."
            ),
            "details": f"Based on GitHub commit {data_commit}",
            "tags": ["geoboundaries", "administrative", "boundary"],
            "citation": (
                "Runfola, D. et al. (2020) geoBoundaries: A global database of "
                "political administrative boundaries. PLoS ONE 15(4): e0231866. "
                "https://doi.org/10.1371/journal.pone.0231866"
            ),
            "source_name": "geoBoundaries",
            "source_url": "geoboundaries.org",
            "other": item.copy(),
            "ingest_src": "geoquery_automated",
            "is_global": False,
            "group_name": f"gb_v6_{iso3.lower()}",
            "group_title": f"{item['boundaryName']} - GeoBoundaries v6",
            "group_class": "parent" if fc_type == "ADM0" else "child",
            "group_level": int(fc_type[3:]),
        }


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
