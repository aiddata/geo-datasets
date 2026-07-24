"""
Drug Cultivation Sites (DRUGDATA 2017-08)

http://www.paivilujala.com/drugdata.html

Rasterizes cannabis, coca bush, and opium poppy cultivation site polygons to
a global 0.01 degree categorical grid (0 = none, 1 = cannabis, 2 = coca bush,
3 = opium poppy, 4 = mix — a cell covered by more than one layer), and builds
a distance-to-nearest-cultivation-site raster from it.

## Manual download

The source zip is downloaded by hand from
http://www.paivilujala.com/drugdata.html and placed at
`<raw_dir>/drugdata.zip`. Layers are then read directly from the zip via
GDAL's `/vsizip/` — no separate extraction step.
"""
from pathlib import Path

import numpy as np
import rasterio
from affine import Affine
from distancerasters import DistanceRaster, rasterize
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

ZIP_SUBDIR = "DRUGDATA ArcGIS files"
# layer name -> category value
LAYERS = {"CANNABIS": 1, "COCA BUSH": 2, "OPIUM POPPY": 3}
MIXED_VALUE = 4
XMIN, XMAX, YMIN, YMAX = -180, 180, -90, 90


class DrugConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    pixel_size: float
    overwrite_categorical_raster: bool
    overwrite_distance_raster: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class Drug(Dataset):

    name = "Drug Cultivation Sites"

    def __init__(self, config: DrugConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.pixel_size = config.pixel_size
        self.overwrite_categorical_raster = config.overwrite_categorical_raster
        self.overwrite_distance_raster = config.overwrite_distance_raster

        self.download_path = self.raw_dir / "drugdata.zip"

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

    def build_categorical_raster(self):
        logger = self.get_logger()

        categorical_path = self.output_dir / "drug_categorical.tif"
        if not self.overwrite_categorical_raster and categorical_path.exists():
            logger.info(f"Categorical raster exists, skipping: {categorical_path}")
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

        output = np.zeros(shape=shape)
        for layer_name, value in LAYERS.items():
            shp_path = f"/vsizip/{{{self.download_path}}}/{ZIP_SUBDIR}/{layer_name}.shp"
            logger.info(f"Rasterizing {shp_path}")
            rv_array, _ = rasterize(shp_path, affine=affine, shape=shape)

            output += rv_array * value
            # any cell covered by more than one layer becomes "mix"
            output = np.where(output > value, MIXED_VALUE, output)

        logger.info(f"Writing categorical raster to {categorical_path}")
        self.write_cog(output, affine, 255, categorical_path)

        return output, affine

    def build_distance_raster(self, categorical, affine):
        logger = self.get_logger()

        distance_path = self.output_dir / "drug_distance.tif"
        if not self.overwrite_distance_raster and distance_path.exists():
            logger.info(f"Distance raster exists, skipping: {distance_path}")
            return

        if categorical is None:
            raise ValueError(
                "Categorical raster not available in memory; set "
                "overwrite_categorical_raster=true to rebuild it alongside "
                "the distance raster."
            )

        logger.info("Calculating distance raster")
        dist = DistanceRaster(
            categorical, affine=affine, conditional=self.raster_conditional
        )

        logger.info(f"Writing distance raster to {distance_path}")
        self.write_cog(dist.dist_array, dist.affine, None, distance_path)

    def main(self):
        logger = self.get_logger()

        logger.info("Building categorical raster")
        categorical, affine = self.build_categorical_raster()

        logger.info("Building distance raster")
        self.build_distance_raster(categorical, affine)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def drug(config: DrugConfiguration):
        Drug(config).run(config.run)


if __name__ == "__main__":
    config = get_config(DrugConfiguration)
    Drug(config).run(config.run)
