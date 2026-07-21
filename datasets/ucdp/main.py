"""
UCDP Georeferenced Event Dataset (GED) — conflict deaths

https://ucdp.uu.se/downloads/

Downloads the GED global CSV (currently v26.1 — UCDP releases a new version
roughly annually; download_url will need bumping when they do) and rasterizes
the "best" fatality estimate per year to a 0.01 degree grid, both combined
across all violence types and broken out by type (state-based, non-state,
one-sided) — one flow producing all four variants from a single download.

The UCDP GED Polygons dataset (v1.1, from 2012), which a separate older
script in this directory used for a binary conflict-occurrence raster, is no
longer listed on the UCDP downloads page and appears to have been retired;
that output is dropped rather than migrated.

Note on aggregation: multiple events can fall in the same grid cell. Points
are rounded to the pixel grid and summed (via groupby) *before*
rasterizing — rasterizing points directly with `attribute=` would keep only
one event's value per pixel rather than the summed total.
"""
import json
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import requests
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

# the ingest json template checked into this dataset's directory
FILTER_INGEST_TEMPLATE = Path(__file__).parent / "ged261_filter_ingest.json"

FIELD_DICT = {
    "year": {
        "display": "Year",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": False,
    },
    "active_year": {
        "display": "Active Year",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "type_of_violence": {
        "display": "Type of Violence",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "conflict_name": {
        "display": "Conflict Name",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "dyad_name": {
        "display": "Dyad Name",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "side_a": {
        "display": "Side A",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "side_b": {
        "display": "Side B",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "number_of_sources": {
        "display": "Number of Sources",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "where_prec": {
        "display": "Location Precision",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    "event_clarity": {
        "display": "Event Clarity",
        "types": ["filters"],
        "filter_type": "categorical",
        "agggregate": True,
    },
    # "date_prec": {
    #     "display": "Date Precision",
    #     "types": ["filters"],
    #     "filter_type": "categorical",
    #     "agggregate": True,
    # },
    # "date_start": {
    #     "display": "Date Start",
    #     "types": ["filters"],
    #     "filter_type": "categorical",
    #     "agggregate": True,
    # },
    # "date_end": {
    #     "display": "Date End",
    #     "types": ["filters"],
    #     "filter_type": "categorical",
    #     "agggregate": True,
    # },
    "deaths_a": {
        "display": "Deaths Side A",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "agggregate": True,
    },
    "deaths_b": {
        "display": "Deaths Side B",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "agggregate": True,
    },
    "deaths_civilians": {
        "display": "Civilian Deaths",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "agggregate": True,
    },
    "deaths_unknown": {
        "display": "Unknown Deaths",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "agggregate": True,
    },
    "best": {
        "display": "Best Estimate of Total Deaths",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "agggregate": True,
    },
    "high": {
        "display": "High Estimate of Total Deaths",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "agggregate": True,
    },
    "low": {
        "display": "Low Estimate of Total Deaths",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "agggregate": True,
    },
}


class UcdpConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_process: bool
    dataset: str

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class Ucdp(Dataset):

    name = "UCDP GED Conflict Deaths"

    def __init__(self, config: UcdpConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_download = config.overwrite_download
        self.overwrite_process = config.overwrite_process
        self.dataset = config.dataset

        self.download_url = f"https://ucdp.uu.se/downloads/ged/{self.dataset}-csv.zip"
        self.download_dst = self.raw_dir / f"{self.dataset}-csv.zip"


    def download(self):
        logger = self.get_logger()

        if not self.overwrite_download and self.download_dst.exists():
            logger.info(f"Download exists, skipping: {self.download_dst}")
            return

        logger.info(f"Downloading {self.download_url} to {self.download_dst}")
        with self.tmp_to_dst_file(self.download_dst, make_dst_dir=True) as tmp:
            with requests.get(self.download_url, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(tmp, "wb") as dst:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        dst.write(chunk)
        logger.info(f"Downloaded {self.download_dst}")

    def load_data(self):
        with ZipFile(self.download_dst) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if len(csv_names) != 1:
                raise ValueError(f"Expected 1 CSV in the zip, found {len(csv_names)}")
            with zf.open(csv_names[0]) as f:
                df = pd.read_csv(f)
        return df

    def process(self):
        logger = self.get_logger()

        output_path = self.output_dir / f"{self.dataset}.gpkg"
        if not self.overwrite_process and output_path.exists():
            logger.info(f"Output exists, skipping: {output_path}")
            return

        raw_gdf = gpd.GeoDataFrame(
            self.df,
            geometry=gpd.points_from_xy(self.df.longitude, self.df.latitude),
            crs="EPSG:4326",
        )

        gdf = raw_gdf[list(FIELD_DICT.keys()) + ["geometry"]].copy()

        gdf.to_file(output_path, layer=self.dataset, driver="GPKG")


    def update_filter_ingest(self):

        logger = self.get_logger()

        if self.gdf is None:
            logger.info("No new data processed; leaving filter_ingest.json as-is")
            return

        with open(FILTER_INGEST_TEMPLATE, "r") as f:
            filter_ingest = json.load(f)

        filter_ingest["other"]["outcomes"] = {k: FIELD_DICT[k]["display"] for k in FIELD_DICT if "outcomes" in FIELD_DICT[k]["types"]}

        filter_ingest["other"]["filters"] = {}

        for field, props in FIELD_DICT.items():
            field_dict = {}
            if "filters" in props["types"]:
                field_dict = {
                    "display": props["display"],
                    "aggregate": props["agggregate"],
                    "type": props["filter_type"],
                }
                if props["filter_type"] == "categorical":
                    field_dict["categories"] = self.gdf[field].unique().tolist()
                elif props["filter_type"] == "range":
                    field_dict["min"] = int(self.gdf[field].min())
                    field_dict["max"] = int(self.gdf[field].max())
                else:
                    raise ValueError(f"Unknown filter_type: {props['filter_type']}")

                filter_ingest["other"]["filters"][field] = field_dict


        logger.info(f"Writing {self.filter_ingest_path}")
        with self.tmp_to_dst_file(self.filter_ingest_path, make_dst_dir=True) as tmp:
            with open(tmp, "w") as f:
                json.dump(filter_ingest, f, indent=4)


    def main(self):
        logger = self.get_logger()

        logger.info("Running UCDP GED download")
        self.download()

        logger.info("Loading GED data")
        self.df = self.load_data()

        logger.info(f"Processing...")
        self.gdf = self.process()

        logger.info("Updating filter_ingest.json")
        self.update_filter_ingest()


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def ucdp(config: UcdpConfiguration):
        Ucdp(config).run(config.run)


if __name__ == "__main__":
    config = get_config(UcdpConfiguration)
    Ucdp(config).run(config.run)
