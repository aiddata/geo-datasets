"""
Distance to Roads (gRoads v1)

https://www.earthdata.nasa.gov/data/catalog/sedac-ciesin-sedac-groads-v1-1.0

Downloads the global gROADS v1 file geodatabase (a single NASA
Earthdata-authenticated zip; requires an `earthdata_token`, same bearer-token
pattern as oco2/ltdr_ndvi), rasterizes the road network to a global 0.01
degree binary grid, and builds a distance-to-nearest-road raster from it.

Because the geodatabase is read directly from the downloaded zip via GDAL's
`/vsizip/`, no separate extraction step is needed.
"""
from pathlib import Path

import distancerasters as dr
import rasterio
import requests
from affine import Affine
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

DOWNLOAD_URL = (
    "https://data.earthdata.nasa.gov/nasa-earth/human-dimensions/sedac-root/"
    "downloads/data/groads/groads-global-roads-open-access-v1/"
    "groads-v1-global-gdb.zip"
)
GDB_LAYER = "Global_Roads"
XMIN, XMAX, YMIN, YMAX = -180, 180, -90, 90


class DistanceToGRoadsConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    pixel_size: float
    earthdata_token: str
    overwrite_download: bool
    overwrite_binary_raster: bool
    overwrite_distance_raster: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class DistanceToGRoads(Dataset):

    name = "Distance to GRoads"

    def __init__(self, config: DistanceToGRoadsConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.pixel_size = config.pixel_size
        self.earthdata_token = config.earthdata_token
        self.overwrite_download = config.overwrite_download
        self.overwrite_binary_raster = config.overwrite_binary_raster
        self.overwrite_distance_raster = config.overwrite_distance_raster

        self.download_path = self.raw_dir / "groads-v1-global-gdb.zip"

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

    def download(self):
        logger = self.get_logger()

        if not self.overwrite_download and self.download_path.exists():
            logger.info(f"Download exists, skipping: {self.download_path}")
            return

        logger.info(f"Downloading {DOWNLOAD_URL} to {self.download_path}")
        headers = {"Authorization": f"Bearer {self.earthdata_token}"}
        with self.tmp_to_dst_file(self.download_path, make_dst_dir=True) as tmp:
            with requests.get(
                DOWNLOAD_URL, headers=headers, stream=True, timeout=60
            ) as response:
                response.raise_for_status()
                with open(tmp, "wb") as dst:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        dst.write(chunk)
        logger.info(f"Downloaded {self.download_path}")

    def build_binary_raster(self):
        logger = self.get_logger()

        binary_path = self.output_dir / "binary" / "groads_binary.tif"
        if not self.overwrite_binary_raster and binary_path.exists():
            logger.info(f"Binary raster exists, skipping: {binary_path}")
            return None, None

        shape = (
            round((YMAX - YMIN) / self.pixel_size),
            round((XMAX - XMIN) / self.pixel_size),
        )
        affine = Affine(self.pixel_size, 0, XMIN, 0, -self.pixel_size, YMAX)

        gdb_path = f"/vsizip/{{{self.download_path}}}/groads-v1-global-gdb/gROADS_v1.gdb"
        logger.info(f"Rasterizing {gdb_path}")
        roads, affine = dr.rasterize(
            gdb_path, layer=GDB_LAYER, affine=affine, shape=shape
        )

        logger.info(f"Writing binary raster to {binary_path}")
        self.write_cog(roads, affine, None, binary_path)

        return roads, affine

    def build_distance_raster(self, roads, affine):
        logger = self.get_logger()

        distance_path = self.output_dir / "groads_distance.tif"
        if not self.overwrite_distance_raster and distance_path.exists():
            logger.info(f"Distance raster exists, skipping: {distance_path}")
            return

        if roads is None:
            raise ValueError(
                "Binary road raster not available in memory; set "
                "overwrite_binary_raster=true to rebuild it alongside the "
                "distance raster."
            )

        logger.info("Calculating distance raster")
        dist = dr.DistanceRaster(
            roads, affine=affine, conditional=self.raster_conditional
        )

        logger.info(f"Writing distance raster to {distance_path}")
        self.write_cog(dist.dist_array, dist.affine, None, distance_path)

    def main(self):
        logger = self.get_logger()

        logger.info("Running gRoads download")
        self.download()

        logger.info("Building binary road raster")
        roads, affine = self.build_binary_raster()

        logger.info("Building distance raster")
        self.build_distance_raster(roads, affine)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def distance_to_groads(config: DistanceToGRoadsConfiguration):
        DistanceToGRoads(config).run(config.run)


if __name__ == "__main__":
    config = get_config(DistanceToGRoadsConfiguration)
    DistanceToGRoads(config).run(config.run)
