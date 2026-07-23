"""
ACLED (Armed Conflict Location & Event Data Project)

https://acleddata.com

ACLED requires registration to access bulk data. Download the full global
dataset from the data export tool and place the CSV in raw_dir before running.

https://acleddata.com/data-export-tool/
"""
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

FILTER_INGEST_TEMPLATE = Path(__file__).parent / "acled_filter_ingest.json"

# optional=True fields are skipped gracefully if not present in the CSV
FIELD_DICT = {
    "year": {
        "display": "Year",
        "types": ["filters"],
        "filter_type": "categorical",
        "aggregate": False,
        "optional": False,
    },
    "disorder_type": {
        "display": "Disorder Type",
        "types": ["filters"],
        "filter_type": "categorical",
        "aggregate": True,
        "optional": True,
    },
    "event_type": {
        "display": "Event Type",
        "types": ["filters"],
        "filter_type": "categorical",
        "aggregate": True,
        "optional": False,
    },
    "sub_event_type": {
        "display": "Sub-Event Type",
        "types": ["filters"],
        "filter_type": "categorical",
        "aggregate": True,
        "optional": False,
    },
    "geo_precision": {
        "display": "Location Precision",
        "types": ["filters"],
        "filter_type": "categorical",
        "aggregate": True,
        "optional": False,
    },
    "time_precision": {
        "display": "Date Precision",
        "types": ["filters"],
        "filter_type": "categorical",
        "aggregate": True,
        "optional": False,
    },
    "event_count": {
        "display": "Event Count",
        "types": ["outcomes"],
        "filter_type": "range",
        "aggregate": True,
        "optional": False,
    },
    "fatalities": {
        "display": "Fatalities",
        "types": ["filters", "outcomes"],
        "filter_type": "range",
        "aggregate": True,
        "optional": False,
    },
}


class ACLEDConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_process: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class ACLED(Dataset):

    name = "ACLED Conflict Events"

    def __init__(self, config: ACLEDConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_process = config.overwrite_process

    def find_csv(self) -> Path:
        logger = self.get_logger()
        csvs = sorted(self.raw_dir.glob("*.csv"))
        if not csvs:
            raise FileNotFoundError(
                f"No CSV found in {self.raw_dir}. "
                "Download the ACLED global dataset from https://acleddata.com/data-export-tool/ "
                "and place the CSV there."
            )
        if len(csvs) > 1:
            logger.warning(f"Multiple CSVs in {self.raw_dir}; using most recent: {csvs[-1].name}")
        return csvs[-1]

    def load_data(self) -> pd.DataFrame:
        logger = self.get_logger()
        csv_path = self.find_csv()
        logger.info(f"Loading {csv_path}")
        df = pd.read_csv(csv_path, low_memory=False)
        df["event_count"] = 1
        logger.info(f"Loaded {len(df):,} events")
        return df

    def process(self):
        logger = self.get_logger()

        output_path = self.output_dir / "acled.gpkg"
        if not self.overwrite_process and output_path.exists():
            logger.info(f"Output exists, skipping: {output_path}")
            return None

        required_cols = [
            f for f, props in FIELD_DICT.items()
            if not props["optional"] and f != "event_count"
        ]
        missing = [c for c in required_cols if c not in self.df.columns]
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        present_optional = [
            f for f, props in FIELD_DICT.items()
            if props["optional"] and f in self.df.columns
        ]
        if present_optional:
            logger.info(f"Optional columns present: {present_optional}")

        keep_cols = (
            required_cols
            + present_optional
            + ["event_count", "latitude", "longitude"]
        )
        # deduplicate while preserving order
        seen = set()
        keep_cols = [c for c in keep_cols if not (c in seen or seen.add(c))]

        gdf = gpd.GeoDataFrame(
            self.df[keep_cols].copy(),
            geometry=gpd.points_from_xy(self.df.longitude, self.df.latitude),
            crs="EPSG:4326",
        )

        logger.info(f"Writing {output_path} ({len(gdf):,} events)")
        with self.tmp_to_dst_file(output_path, make_dst_dir=True) as tmp:
            gdf.to_file(tmp, layer="acled", driver="GPKG")

        return gdf

    def update_filter_ingest(self):
        logger = self.get_logger()

        if self.gdf is None:
            logger.info("Output exists and overwrite_process=false; leaving filter_ingest as-is")
            return

        with open(FILTER_INGEST_TEMPLATE, "r") as f:
            filter_ingest = json.load(f)

        filter_ingest["other"]["outcomes"] = {
            k: FIELD_DICT[k]["display"]
            for k in FIELD_DICT
            if "outcomes" in FIELD_DICT[k]["types"]
        }

        filter_ingest["other"]["filters"] = {}

        for field, props in FIELD_DICT.items():
            if "filters" not in props["types"]:
                continue
            if field not in self.gdf.columns:
                continue
            field_dict = {
                "display": props["display"],
                "aggregate": props["aggregate"],
                "type": props["filter_type"],
            }
            if props["filter_type"] == "categorical":
                field_dict["categories"] = sorted(
                    self.gdf[field].dropna().unique().tolist()
                )
            elif props["filter_type"] == "range":
                field_dict["min"] = int(self.gdf[field].min())
                field_dict["max"] = int(self.gdf[field].max())
            filter_ingest["other"]["filters"][field] = field_dict

        logger.info(f"Writing {FILTER_INGEST_TEMPLATE}")
        with self.tmp_to_dst_file(FILTER_INGEST_TEMPLATE, make_dst_dir=True) as tmp:
            with open(tmp, "w") as f:
                json.dump(filter_ingest, f, indent=4)

    def main(self):
        logger = self.get_logger()

        logger.info("Loading data...")
        self.df = self.load_data()

        logger.info("Processing...")
        self.gdf = self.process()

        logger.info("Updating ingest JSON...")
        self.update_filter_ingest()


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def acled(config: ACLEDConfiguration):
        ACLED(config).run(config.run)


if __name__ == "__main__":
    config = get_config(ACLEDConfiguration)
    ACLED(config).run(config.run)
