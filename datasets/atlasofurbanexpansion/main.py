"""
Atlas of Urban Expansion (2016) — 200-city sample

http://www.atlasofurbanexpansion.org

Downloads per-city metadata (area/density and blocks/roads tables) and
per-city GIS zip archives for the 200-city sample, extracts the study area
and urban-edge-at-time boundaries (t1 = 1990, t2 = 2000, t3 = 2014), dissolves
each into a single (multi)polygon, reprojects to EPSG:4326, and writes one
GeoJSON FeatureCollection per level with the merged metadata as properties.

Column reordering (previously a separate `col_order.py` step, tied to the old
GeoQuery ingest pipeline) is not needed here and was dropped.
"""
import json
import math
from pathlib import Path
from urllib.parse import quote
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import requests
from pydantic import field_validator
from shapely.geometry import mapping, shape
from shapely.geometry.multipolygon import MultiPolygon
from shapely.ops import unary_union

from data_manager import BaseDatasetConfiguration, Dataset, get_config

AREA_TABLE_URL = "http://atlasofurbanexpansion.org/file-manager/userfiles/data_page/Areas_and_Densities_Tables/Areas_and_Densities_Table_1.csv"
ROAD_TABLE_URL = "http://atlasofurbanexpansion.org/file-manager/userfiles/data_page/Blocks_and_Roads_Tables/Blocks_and_Roads_Table_1.csv"
ZIP_URL_BASE = "http://www.atlasofurbanexpansion.org/file-manager/userfiles/data_page/Phase I GIS"

# city name corrections between the metadata tables and the GIS archive names
CITY_NAME_FIXES = [
    ("Pematangtiantar", "Pematangsiantar"),
    ("Tebessa ", "Tebessa"),
    ("Tianjin,  Tianjin", "Tianjin, Tianjin"),
]
# GIS archive filename doesn't match the metadata city name for this one city
ZIP_FILENAME_FIXES = {"Changzhi,_Hunan.zip": "Changzhi_Shanxi.zip"}

LEVELS = ["studyArea", "urban_edge_t1", "urban_edge_t2", "urban_edge_t3"]


def clean_columns(header_df):
    """Merge the table's two-row header into single column names."""
    isnan = lambda x: x == "nan"
    columns = []
    prev_top = "nan"
    for i in range(header_df.shape[1]):
        top = str(header_df.iloc[0, i])
        bot = str(header_df.iloc[1, i])
        if not isnan(top) and isnan(bot):
            cname = top
        elif isnan(top) and not isnan(bot):
            cname = f"{prev_top} {bot}"
        elif not isnan(top) and not isnan(bot):
            cname = f"{top} {bot}"
        else:
            raise Exception(f"Error for column {i} (top: {top}, bot: {bot}, prev_top: {prev_top})")
        if not isnan(top):
            prev_top = top
        columns.append(cname)
    return columns


class AtlasOfUrbanExpansionConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_process: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class AtlasOfUrbanExpansion(Dataset):

    name = "Atlas of Urban Expansion"

    def __init__(self, config: AtlasOfUrbanExpansionConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_download = config.overwrite_download
        self.overwrite_process = config.overwrite_process

        self.shps_dir = self.raw_dir / "shps"
        self.metadata_path = self.raw_dir / "metadata.csv"
        self.metadata_df = None

    def download_file(self, url, dst_path):
        logger = self.get_logger()
        if not self.overwrite_download and dst_path.exists():
            logger.info(f"Download exists, skipping: {dst_path}")
            return
        logger.info(f"Downloading {url} to {dst_path}")
        with self.tmp_to_dst_file(dst_path, make_dst_dir=True) as tmp:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            with open(tmp, "wb") as dst:
                dst.write(response.content)

    def build_metadata(self):
        """Download and merge the area/density and blocks/roads tables into
        a single metadata table, indexed by city name."""
        logger = self.get_logger()

        area_path = self.raw_dir / "Areas_and_Densities_Table_1.csv"
        road_path = self.raw_dir / "Blocks_and_Roads_Table_1.csv"
        self.download_file(AREA_TABLE_URL, area_path)
        self.download_file(ROAD_TABLE_URL, road_path)

        area_df = pd.read_csv(
            area_path, header=None, quotechar='"', na_values="", keep_default_na=False, encoding="utf-8"
        )
        road_df = pd.read_csv(
            road_path, header=None, quotechar='"', na_values="", keep_default_na=False, encoding="utf-8"
        )

        # drop trailing empty columns
        area_df = area_df.drop(area_df.columns[[79, 80]], axis=1)

        area_header = area_df.iloc[[0, 1]].copy(deep=True)
        road_header = road_df.iloc[[0, 1]].copy(deep=True)

        area_df = area_df.drop([0, 1], axis=0)
        road_df = road_df.drop([0, 1], axis=0)

        area_df = area_df.loc[area_df[0].notnull()]
        road_df = road_df.loc[road_df[0].notnull()]

        for old, new in CITY_NAME_FIXES:
            area_df.loc[area_df[0] == old, 0] = new
            road_df.loc[road_df[0] == old, 0] = new

        area_df.columns = clean_columns(area_header)
        road_df.columns = clean_columns(road_header)

        merge_field = "City Name"
        extra_cols = [merge_field] + [c for c in road_df.columns if c not in area_df.columns]
        metadata_df = pd.merge(area_df, road_df[extra_cols], on=merge_field)
        metadata_df = metadata_df.reset_index(drop=True)
        metadata_df.to_csv(self.metadata_path, index=False, encoding="utf-8")

        logger.info(f"Built metadata for {len(metadata_df)} cities")
        self.metadata_df = metadata_df

    def get_zip_url_and_path(self, city_name):
        city_us = city_name.replace(" ", "_")
        zip_filename = f"{city_us}.zip"
        zip_filename = ZIP_FILENAME_FIXES.get(zip_filename, zip_filename)
        url = quote(ZIP_URL_BASE, safe=":/") + "/" + quote(zip_filename, safe=",")
        dst_path = self.raw_dir / "zip" / zip_filename
        return url, dst_path

    def download_and_extract_city(self, city_name):
        logger = self.get_logger()
        city_us = city_name.replace(" ", "_")

        zip_url, zip_path = self.get_zip_url_and_path(city_name)
        try:
            self.download_file(zip_url, zip_path)
        except Exception as e:
            logger.warning(f"Failed to download {zip_url}: {e}")
            return

        valid_file_str = ["studyArea.", "urban_edge_t"]
        with ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if any(s in m for s in valid_file_str)]
            zf.extractall(self.shps_dir, members)
        logger.info(f"Extracted {city_us}")

    def get_level_geometry(self, city_name, level):
        """Read, dissolve, and reproject a city's boundary for one level."""
        city_us = city_name.replace(" ", "_")

        if level == "studyArea":
            shp_path = self.shps_dir / city_us / f"{city_us}_studyArea.shp"
        else:
            shp_path = self.shps_dir / city_us / f"{level}.shp"

        if not shp_path.exists():
            return None

        gdf = gpd.read_file(shp_path)
        if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        geom = unary_union(gdf.geometry).buffer(0)
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])

        return geom

    def build_level_feature(self, args):
        idx, row, level = args
        logger = self.get_logger()

        city_name = row["City Name"]
        geom = self.get_level_geometry(city_name, level)
        if geom is None:
            logger.warning(f"No {level} boundary found for {city_name}")
            return None

        props = row.to_dict()
        for k, v in props.items():
            if isinstance(v, float) and math.isnan(v):
                props[k] = ""

        return {
            "id": idx,
            "type": "Feature",
            "geometry": mapping(geom),
            "properties": props,
        }

    def build_level(self, level):
        logger = self.get_logger()

        output_path = self.output_dir / f"{level}.geojson"
        if not self.overwrite_process and output_path.exists():
            logger.info(f"Output exists, skipping: {output_path}")
            return

        logger.info(f"Building level: {level}")
        args = [(idx, row, level) for idx, row in self.metadata_df.iterrows()]
        features_run = self.run_tasks(self.build_level_feature, [[a] for a in args])
        self.log_run(features_run)
        features = [f for f in features_run.results() if f is not None]

        feature_collection = {"type": "FeatureCollection", "features": features}

        with self.tmp_to_dst_file(output_path, make_dst_dir=True) as tmp:
            with open(tmp, "w") as f:
                json.dump(feature_collection, f)

        logger.info(f"Wrote {len(features)} features to {output_path}")

    def main(self):
        logger = self.get_logger()

        logger.info("Building metadata")
        self.build_metadata()

        logger.info("Downloading and extracting per-city archives")
        cities = self.metadata_df["City Name"].tolist()
        extract_run = self.run_tasks(self.download_and_extract_city, [[c] for c in cities])
        self.log_run(extract_run)

        for level in LEVELS:
            self.build_level(level)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def atlasofurbanexpansion(config: AtlasOfUrbanExpansionConfiguration):
        AtlasOfUrbanExpansion(config).run(config.run)


if __name__ == "__main__":
    config = get_config(AtlasOfUrbanExpansionConfiguration)
    AtlasOfUrbanExpansion(config).run(config.run)
