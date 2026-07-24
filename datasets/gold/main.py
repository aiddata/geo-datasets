"""
Gold Deposits (GOLDATA v1.2)

https://www.researchgate.net/publication/281849073_GOLDATA_12_v

Rasterizes large-scale (L), small-scale (S), and non-lootable (NL) gold
deposit polygons to a global 0.01 degree categorical grid (0 = none,
1 = lootable/large-scale, 2 = surface/small-scale, 3 = non-lootable,
4 = mix — a cell covered by more than one layer), and builds a
distance-to-nearest-deposit raster from the lootable (L + S) layers, matching
the original script's ordering (distance is computed before the non-lootable
layer is folded into the categorical output).

## Manual download

ResearchGate blocks automated requests, so the source zip is downloaded by
hand from
https://www.researchgate.net/publication/281849073_GOLDATA_12_v, extracted,
and each layer's shapefile placed at `<raw_dir>/<layer>/<layer>.shp` — see
README.md.
"""
from pathlib import Path

import numpy as np
import rasterio
from affine import Affine
from distancerasters import DistanceRaster, rasterize
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

# layer name -> category value; L/S computed first (used for the distance
# raster), NL folded in afterward
LOOTABLE_LAYERS = {"dGOLD_L": 1, "dGOLD_S": 2}
NONLOOTABLE_LAYER = "dGOLD_NL"
NONLOOTABLE_VALUE = 3
MIXED_VALUE = 4
XMIN, XMAX, YMIN, YMAX = -180, 180, -90, 90


class GoldConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    pixel_size: float
    overwrite_categorical_raster: bool
    overwrite_distance_raster: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class Gold(Dataset):

    name = "Gold Deposits"

    def __init__(self, config: GoldConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.pixel_size = config.pixel_size
        self.overwrite_categorical_raster = config.overwrite_categorical_raster
        self.overwrite_distance_raster = config.overwrite_distance_raster

    def raster_conditional(self, rarray):
        return rarray > 0

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

    def layer_shp_path(self, layer_name):
        return self.raw_dir / layer_name / f"{layer_name}.shp"

    def rasterize_lootable(self):
        logger = self.get_logger()

        shape = (
            round((YMAX - YMIN) / self.pixel_size),
            round((XMAX - XMIN) / self.pixel_size),
        )
        affine = Affine(self.pixel_size, 0, XMIN, 0, -self.pixel_size, YMAX)

        output = np.zeros(shape=shape)
        for layer_name, value in LOOTABLE_LAYERS.items():
            shp_path = self.layer_shp_path(layer_name)
            if not shp_path.exists():
                raise FileNotFoundError(
                    f"{shp_path} not found — see README.md for the manual "
                    "download step required before running this flow."
                )
            logger.info(f"Rasterizing {shp_path}")
            rv_array, _ = rasterize(str(shp_path), affine=affine, shape=shape)

            output += rv_array * value
            output = np.where(output > value, MIXED_VALUE, output)

        return output, affine

    def build_distance_raster(self, lootable, affine):
        logger = self.get_logger()

        distance_path = self.output_dir / "gold_distance.tif"
        if not self.overwrite_distance_raster and distance_path.exists():
            logger.info(f"Distance raster exists, skipping: {distance_path}")
            return

        logger.info("Calculating distance raster")
        dist = DistanceRaster(
            lootable, affine=affine, conditional=self.raster_conditional
        )

        logger.info(f"Writing distance raster to {distance_path}")
        self.write_cog(dist.dist_array, dist.affine, None, distance_path)

    def build_categorical_raster(self, output, affine):
        logger = self.get_logger()

        categorical_path = self.output_dir / "gold_categorical.tif"
        if not self.overwrite_categorical_raster and categorical_path.exists():
            logger.info(f"Categorical raster exists, skipping: {categorical_path}")
            return

        shp_path = self.layer_shp_path(NONLOOTABLE_LAYER)
        if not shp_path.exists():
            raise FileNotFoundError(
                f"{shp_path} not found — see README.md for the manual "
                "download step required before running this flow."
            )

        shape = output.shape
        logger.info(f"Rasterizing {shp_path}")
        rv_array, _ = rasterize(str(shp_path), affine=affine, shape=shape)

        output = output + rv_array * NONLOOTABLE_VALUE
        output = np.where(output > NONLOOTABLE_VALUE, MIXED_VALUE, output)

        logger.info(f"Writing categorical raster to {categorical_path}")
        self.write_cog(output, affine, 255, categorical_path)

    def main(self):
        logger = self.get_logger()

        logger.info("Rasterizing lootable (L/S) layers")
        lootable, affine = self.rasterize_lootable()

        logger.info("Building distance raster")
        self.build_distance_raster(lootable, affine)

        logger.info("Building categorical raster")
        self.build_categorical_raster(lootable, affine)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def gold(config: GoldConfiguration):
        Gold(config).run(config.run)


if __name__ == "__main__":
    config = get_config(GoldConfiguration)
    Gold(config).run(config.run)
