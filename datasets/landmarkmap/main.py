"""
LandMark: Indigenous Peoples' Lands & Territories and Local Community Lands

https://landmarkmap.org/data-methods/access-data

Manually downloaded (no automated download step - see README). Combines the
source archive's point and polygon layers (points buffered to small
polygons) into a single GeoPackage, written to two locations so it can be
ingested twice: once as a "feature" type, once as a "boundary" type.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

from data_manager import BaseDatasetConfiguration, Dataset, get_config

OUTPUT_CRS = "EPSG:4326"


class LandMarkMapConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    feature_output_dir: str
    boundary_output_dir: str
    buffer_meters: float
    overwrite_process: bool


class LandMarkMap(Dataset):
    name = "LandMark: Indigenous Peoples' and Community Lands"

    def __init__(self, config: LandMarkMapConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.feature_output_dir = Path(config.feature_output_dir)
        self.boundary_output_dir = Path(config.boundary_output_dir)
        self.buffer_meters = config.buffer_meters
        self.overwrite_process = config.overwrite_process

        self.feature_output_path = self.feature_output_dir / "landmarkmap.gpkg"
        self.boundary_output_path = self.boundary_output_dir / "landmarkmap.gpkg"

    def find_shapefile(self, pattern: str) -> Path:
        """Find the single shapefile matching a glob pattern under raw_dir/shp.

        Matches by pattern rather than a hardcoded version string, since
        LandMark's filenames embed a release version (e.g. v202606) that
        changes with each data update.
        """
        matches = list((self.raw_dir / "shp").glob(pattern))
        if len(matches) == 0:
            raise FileNotFoundError(
                f"No shapefile matching {pattern!r} found in {self.raw_dir / 'shp'}. "
                "See README for manual download instructions."
            )
        if len(matches) > 1:
            raise Exception(f"Multiple shapefiles matching {pattern!r} found: {matches}")
        return matches[0]

    def build_combined_gdf(self) -> gpd.GeoDataFrame:
        """Read points and polygons, buffer points to polygons, and combine
        both layers into a single GeoDataFrame in EPSG:4326."""
        logger = self.get_logger()

        point_shp = self.find_shapefile("LandMark_IP_LC_point_public_v*.shp")
        logger.info(f"Reading points from {point_shp}")
        points = gpd.read_file(point_shp, engine="pyogrio")
        logger.info(f"Loaded {len(points)} point features")

        # source CRS (EPSG:3857) is already in meters, so buffering here
        # doesn't require reprojecting first
        points["geometry"] = points.buffer(self.buffer_meters)

        poly_shp = self.find_shapefile("LandMark_IP_LC_poly_public_v*.shp")
        logger.info(f"Reading polygons from {poly_shp}")
        polygons = gpd.read_file(poly_shp, engine="pyogrio", force_2d=True)
        logger.info(f"Loaded {len(polygons)} polygon features")

        combined = gpd.GeoDataFrame(
            pd.concat([points, polygons], ignore_index=True), crs=points.crs
        )
        combined = combined.to_crs(OUTPUT_CRS)
        logger.info(f"Combined into {len(combined)} total features")
        return combined

    def main(self):
        logger = self.get_logger()

        if (
            self.feature_output_path.exists()
            and self.boundary_output_path.exists()
            and not self.overwrite_process
        ):
            logger.info("Outputs exist, skipping")
            return

        gdf = self.build_combined_gdf()

        for output_path in (self.feature_output_path, self.boundary_output_path):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            gdf.to_file(output_path, driver="GPKG")
            logger.info(f"Saved {output_path}")


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def landmarkmap(config: LandMarkMapConfiguration):
        LandMarkMap(config).run(config.run)


if __name__ == "__main__":
    config = get_config(LandMarkMapConfiguration)
    LandMarkMap(config).run(config.run)
