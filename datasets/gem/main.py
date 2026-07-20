"""
Gemstone Deposits (GEMDATA 2017-08)

http://www.paivilujala.com/gemdata.html

Rasterizes gemstone deposit points (ruby, sapphire, emerald, aquamarine, and
other gemstones, excluding diamonds) to a global 0.01 degree binary grid, and
builds a distance-to-nearest-deposit raster from it.

## Manual download

The source zip is downloaded by hand from
http://www.paivilujala.com/gemdata.html and placed at
`<raw_dir>/gemdata.zip`. The geodatabase is then read directly from the zip
via GDAL's `/vsizip/` — no separate extraction step.
"""
from pathlib import Path

import distancerasters as dr
import rasterio
from affine import Affine
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

SHP_NAME = "GEMDATA.shp"
XMIN, XMAX, YMIN, YMAX = -180, 180, -90, 90


class GemConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    pixel_size: float
    overwrite_binary_raster: bool
    overwrite_distance_raster: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class Gem(Dataset):

    name = "Gemstone Deposits"

    def __init__(self, config: GemConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.pixel_size = config.pixel_size
        self.overwrite_binary_raster = config.overwrite_binary_raster
        self.overwrite_distance_raster = config.overwrite_distance_raster

        self.download_path = self.raw_dir / "gemdata.zip"

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

        binary_path = self.output_dir / "binary" / "gemstone_binary.tif"
        if not self.overwrite_binary_raster and binary_path.exists():
            logger.info(f"Binary raster exists, skipping: {binary_path}")
            return None, None

        if not self.download_path.exists():
            raise FileNotFoundError(
                f"{self.download_path} not found — see README.md for the "
                "manual download step required before running this flow."
            )

        shape = (
            round((YMAX - YMIN) / self.pixel_size),
            round((XMAX - XMIN) / self.pixel_size),
        )
        affine = Affine(self.pixel_size, 0, XMIN, 0, -self.pixel_size, YMAX)

        shp_path = f"/vsizip/{{{self.download_path}}}/{SHP_NAME}"
        logger.info(f"Rasterizing {shp_path}")
        gem, affine = dr.rasterize(shp_path, affine=affine, shape=shape)

        logger.info(f"Writing binary raster to {binary_path}")
        self.write_cog(gem, affine, None, binary_path)

        return gem, affine

    def build_distance_raster(self, gem, affine):
        logger = self.get_logger()

        distance_path = self.output_dir / "gemstone_distance.tif"
        if not self.overwrite_distance_raster and distance_path.exists():
            logger.info(f"Distance raster exists, skipping: {distance_path}")
            return

        if gem is None:
            raise ValueError(
                "Binary raster not available in memory; set "
                "overwrite_binary_raster=true to rebuild it alongside the "
                "distance raster."
            )

        logger.info("Calculating distance raster")
        dist = dr.DistanceRaster(
            gem, affine=affine, conditional=self.raster_conditional
        )

        logger.info(f"Writing distance raster to {distance_path}")
        self.write_cog(dist.dist_array, dist.affine, None, distance_path)

    def main(self):
        logger = self.get_logger()

        logger.info("Building binary raster")
        gem, affine = self.build_binary_raster()

        logger.info("Building distance raster")
        self.build_distance_raster(gem, affine)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def gem(config: GemConfiguration):
        Gem(config).run(config.run)


if __name__ == "__main__":
    config = get_config(GemConfiguration)
    Gem(config).run(config.run)
