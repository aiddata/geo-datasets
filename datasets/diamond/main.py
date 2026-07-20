"""
Diamond Resources (DIADATA)

https://www.prio.org/data/10

Rasterizes diamond deposit points to a global 0.01 degree binary grid, and
builds a distance-to-nearest-deposit raster from it.

## Manual download

PRIO's download link redirects through an organizational (Microsoft Entra)
login, so it can't be fetched programmatically. Download the zip from
https://www.prio.org/data/10, extract it, and place `DIADATA.shp` (and its
sidecar files) directly in `<raw_dir>/` — see README.md.
"""
from pathlib import Path

import distancerasters as dr
import rasterio
from affine import Affine
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

SHP_NAME = "DIADATA.shp"
XMIN, XMAX, YMIN, YMAX = -180, 180, -90, 90


class DiamondConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    pixel_size: float
    overwrite_binary_raster: bool
    overwrite_distance_raster: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class Diamond(Dataset):

    name = "Diamond Resources"

    def __init__(self, config: DiamondConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.pixel_size = config.pixel_size
        self.overwrite_binary_raster = config.overwrite_binary_raster
        self.overwrite_distance_raster = config.overwrite_distance_raster

        self.shp_path = self.raw_dir / SHP_NAME

    def raster_conditional(self, rarray):
        return rarray == 1

    def write_cog(self, array, affine, nodata, dst_path):
        meta = {
            "count": 1,
            "crs": "EPSG:4326",
            "dtype": "float64",
            "transform": affine,
            "driver": "COG",
            "compress": "LZW",
            "height": array.shape[0],
            "width": array.shape[1],
            "nodata": nodata,
        }
        with self.tmp_to_dst_file(
            dst_path, make_dst_dir=True, validate_cog=True
        ) as tmp:
            with rasterio.open(tmp, "w", **meta) as dst:
                dst.write(array.astype("float64"), 1)

    def build_binary_raster(self):
        logger = self.get_logger()

        binary_path = self.output_dir / "binary" / "diamond_binary.tif"
        if not self.overwrite_binary_raster and binary_path.exists():
            logger.info(f"Binary raster exists, skipping: {binary_path}")
            return None, None

        if not self.shp_path.exists():
            raise FileNotFoundError(
                f"{self.shp_path} not found — see README.md for the manual "
                "download step required before running this flow."
            )

        shape = (
            round((YMAX - YMIN) / self.pixel_size),
            round((XMAX - XMIN) / self.pixel_size),
        )
        affine = Affine(self.pixel_size, 0, XMIN, 0, -self.pixel_size, YMAX)

        logger.info(f"Rasterizing {self.shp_path}")
        diamond, affine = dr.rasterize(
            str(self.shp_path), affine=affine, shape=shape
        )

        logger.info(f"Writing binary raster to {binary_path}")
        self.write_cog(diamond, affine, None, binary_path)

        return diamond, affine

    def build_distance_raster(self, diamond, affine):
        logger = self.get_logger()

        distance_path = self.output_dir / "diamond_distance.tif"
        if not self.overwrite_distance_raster and distance_path.exists():
            logger.info(f"Distance raster exists, skipping: {distance_path}")
            return

        if diamond is None:
            raise ValueError(
                "Binary raster not available in memory; set "
                "overwrite_binary_raster=true to rebuild it alongside the "
                "distance raster."
            )

        logger.info("Calculating distance raster")
        dist = dr.DistanceRaster(
            diamond, affine=affine, conditional=self.raster_conditional
        )

        logger.info(f"Writing distance raster to {distance_path}")
        self.write_cog(dist.dist_array, dist.affine, None, distance_path)

    def main(self):
        logger = self.get_logger()

        logger.info("Building binary raster")
        diamond, affine = self.build_binary_raster()

        logger.info("Building distance raster")
        self.build_distance_raster(diamond, affine)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def diamond(config: DiamondConfiguration):
        Diamond(config).run(config.run)


if __name__ == "__main__":
    config = get_config(DiamondConfiguration)
    Diamond(config).run(config.run)
