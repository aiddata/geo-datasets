"""
Prepare GCDF v3.0.1 as a dataset for ingestion into GeoQuery.

Downloads the pre-built project geometries (buffered polygons) from the GCDF
GitHub release, renames/subsets columns to the fields GeoQuery filters and
aggregates on, and saves as a GeoPackage. Also refreshes filter_ingest.json's
`other.filters` (year ranges, project status/sector categories) from the
downloaded data, so the ingest metadata shipped alongside the output always
reflects what's actually in it.
"""
import json
from pathlib import Path

import geopandas as gpd
import requests
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

FILTER_INGEST_TEMPLATE = Path(__file__).parent / "filter_ingest.json"

DOWNLOAD_URL = "https://github.com/aiddata/gcdf-geospatial-data/releases/download/v3.0.1/all_combined_global.gpkg.zip"
GPKG_NAME = "all_combined_global.gpkg"
LAYER_NAME = "all_combined_global"
RENAME_DICT = {
    "Amount.(Constant.USD.2021)": "Commitment Value",
    "Sector.Name": "Sector Name",
    "Status": "Project Status",
    "Commitment.Year": "Commitment Year",
    "Completion.Year": "Completion Year",
    "geometry": "geometry",
}


class GcdfV301DynamicConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_process: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class GcdfV301Dynamic(Dataset):

    name = "GCDF v3.0.1 Dynamic"

    def __init__(self, config: GcdfV301DynamicConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_download = config.overwrite_download
        self.overwrite_process = config.overwrite_process

        self.download_path = self.raw_dir / "all_combined_global.gpkg.zip"
        self.output_path = self.output_dir / "gcdf_v301_dynamic.gpkg"

    def download(self):
        logger = self.get_logger()

        if not self.overwrite_download and self.download_path.exists():
            logger.info(f"Download exists, skipping: {self.download_path}")
            return

        logger.info(f"Downloading {DOWNLOAD_URL} to {self.download_path}")
        with self.tmp_to_dst_file(self.download_path, make_dst_dir=True) as tmp:
            with requests.get(DOWNLOAD_URL, stream=True, timeout=120) as response:
                response.raise_for_status()
                with open(tmp, "wb") as dst:
                    for chunk in response.iter_content(chunk_size=1024 * 1024 * 8):
                        dst.write(chunk)
        logger.info(f"Downloaded {self.download_path}")

    def process(self):
        logger = self.get_logger()

        if not self.overwrite_process and self.output_path.exists():
            logger.info(f"Output exists, skipping: {self.output_path}")
            return None

        vsizip_path = f"/vsizip/{{{self.download_path}}}/{GPKG_NAME}"
        logger.info(f"Reading {vsizip_path}")
        raw_gdf = gpd.read_file(vsizip_path, layer=LAYER_NAME)

        gdf = raw_gdf.rename(columns=RENAME_DICT)
        gdf = gdf[list(RENAME_DICT.values())].copy()

        logger.info(f"Writing {self.output_path}")
        with self.tmp_to_dst_file(self.output_path, make_dst_dir=True) as tmp:
            gdf.to_file(tmp, layer="gcdf_v301_dynamic", driver="GPKG")

        return gdf

    def update_filter_ingest(self, gdf):
        logger = self.get_logger()

        if gdf is None:
            logger.info("No new data processed; leaving filter_ingest.json as-is")
            return

        with open(FILTER_INGEST_TEMPLATE, "r") as f:
            filter_ingest = json.load(f)

        filter_ingest["other"]["outcomes"] = {
            "Commitment Value": "Commitment Value",
        }

        filter_ingest["other"]["filters"] = {
            "Commitment Year": {
                "display": "Commitment Year",
                "aggregate": True,
                "type": "range",
                "min": int(gdf["Commitment Year"].min()),
                "max": int(gdf["Commitment Year"].max()),
            },
            "Completion Year": {
                "display": "Completion Year",
                "aggregate": True,
                "type": "range",
                "min": int(gdf["Completion Year"].min()),
                "max": int(gdf["Completion Year"].max()),
            },
            "Project Status": {
                "display": "Project Status",
                "aggregate": True,
                "type": "categorical",
                "categories": sorted(gdf["Project Status"].unique().tolist()),
            },
            "Sector Name": {
                "display": "Sector Name",
                "aggregate": True,
                "type": "categorical",
                "categories": sorted(gdf["Sector Name"].unique().tolist()),
            },
        }

        logger.info(f"Writing {FILTER_INGEST_TEMPLATE}")
        with self.tmp_to_dst_file(FILTER_INGEST_TEMPLATE, make_dst_dir=True) as tmp:
            with open(tmp, "w") as f:
                json.dump(filter_ingest, f, indent=4)

    def main(self):
        logger = self.get_logger()

        logger.info("Running GCDF v3.0.1 download")
        self.download()

        logger.info("Processing GCDF v3.0.1 data")
        gdf = self.process()

        logger.info("Updating filter_ingest.json")
        self.update_filter_ingest(gdf)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def gcdf_v301_dynamic(config: GcdfV301DynamicConfiguration):
        GcdfV301Dynamic(config).run(config.run)


if __name__ == "__main__":
    config = get_config(GcdfV301DynamicConfiguration)
    GcdfV301Dynamic(config).run(config.run)
