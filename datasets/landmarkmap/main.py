"""
LandMark: Indigenous Peoples' Lands & Territories and Local Community Lands

https://landmarkmap.org/data-methods/access-data

Manually downloaded (no automated download step - see README). Produces two
outputs from the same source archive:
  - points, buffered to small polygons, as a "feature" type dataset
  - the polygon layer, as-is, as a "boundary" type dataset
"""

from pathlib import Path

import geopandas as gpd

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

        self.point_output_path = self.feature_output_dir / "landmarkmap_points.gpkg"
        self.poly_output_path = self.boundary_output_dir / "landmarkmap_polygons.gpkg"

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

    def process_points(self):
        """Buffer point features to small polygons and write a GeoPackage."""
        logger = self.get_logger()

        if self.point_output_path.exists() and not self.overwrite_process:
            logger.info(f"Output exists, skipping: {self.point_output_path}")
            return

        point_shp = self.find_shapefile("LandMark_IP_LC_point_public_v*.shp")
        logger.info(f"Reading points from {point_shp}")
        gdf = gpd.read_file(point_shp, engine="pyogrio")
        logger.info(f"Loaded {len(gdf)} point features")

        # source CRS (EPSG:3857) is already in meters, so buffering here
        # doesn't require reprojecting first
        gdf["geometry"] = gdf.buffer(self.buffer_meters)
        gdf = gdf.to_crs(OUTPUT_CRS)

        self.point_output_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(self.point_output_path, driver="GPKG")
        logger.info(f"Saved {self.point_output_path}")

    def process_polygons(self):
        """Reproject the polygon layer and write a GeoPackage."""
        logger = self.get_logger()

        if self.poly_output_path.exists() and not self.overwrite_process:
            logger.info(f"Output exists, skipping: {self.poly_output_path}")
            return

        poly_shp = self.find_shapefile("LandMark_IP_LC_poly_public_v*.shp")
        logger.info(f"Reading polygons from {poly_shp}")
        gdf = gpd.read_file(poly_shp, engine="pyogrio", force_2d=True)
        logger.info(f"Loaded {len(gdf)} polygon features")

        gdf = gdf.to_crs(OUTPUT_CRS)

        self.poly_output_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(self.poly_output_path, driver="GPKG")
        logger.info(f"Saved {self.poly_output_path}")

    def main(self):
        logger = self.get_logger()

        logger.info("Processing points -> feature output")
        self.process_points()

        logger.info("Processing polygons -> boundary output")
        self.process_polygons()


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
