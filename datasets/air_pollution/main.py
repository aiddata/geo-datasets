"""
Ambient Air Pollution (GBD 2013) — ozone and PM2.5

https://pubs.acs.org/doi/10.1021/acs.est.5b03709

Ozone and PM2.5 (fus_calibrated) point estimates, rasterized to 0.1 degree
resolution per year. Source data requires a manual download step — see
README.md — and this flow expects `GBD2013final.csv` to already be present
at `<raw_dir>/GBD2013final.csv`; it only handles rasterization.
"""
import itertools
from pathlib import Path

import geopandas as gpd
import pandas as pd
import rasterio
from distancerasters import rasterize
from pydantic import field_validator
from shapely.geometry import Point

from data_manager import BaseDatasetConfiguration, Dataset, get_config

PIXEL_SIZE = 0.1
COL_PREFIXES = ["o3", "fus_calibrated"]
YEARS = [1990, 1995, 2000, 2005, 2010, 2011, 2012, 2013]


class AirPollutionConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_process: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class AirPollution(Dataset):

    name = "Ambient Air Pollution 2013"

    def __init__(self, config: AirPollutionConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_process = config.overwrite_process

        self.input_csv = self.raw_dir / "GBD2013final.csv"
        self.field_list = [
            f"{prefix}_{year}" for prefix, year in itertools.product(COL_PREFIXES, YEARS)
        ]

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

    def process(self, field):
        logger = self.get_logger()

        # field[:-5] strips the trailing "_YYYY" to get the pollutant subdir
        output_path = self.output_dir / field[:-5] / f"{field}.tif"

        if not self.overwrite_process and output_path.exists():
            logger.info(f"Output exists, skipping: {output_path}")
            return

        df = pd.read_csv(self.input_csv, delimiter=",", encoding="utf-8")
        df = df[[field, "x", "y"]]
        df["geometry"] = df.apply(lambda z: Point(z["x"], z["y"]), axis=1)
        gdf = gpd.GeoDataFrame(df)

        logger.info(f"Rasterizing {field} to {output_path}")
        array, affine = rasterize(
            gdf,
            attribute=field,
            pixel_size=PIXEL_SIZE,
            bounds=gdf.geometry.total_bounds,
            fill=-1,
            nodata=-1,
        )
        self.write_cog(array, affine, -1, output_path)
        logger.info(f"Wrote {output_path}")

    def main(self):
        logger = self.get_logger()

        if not self.input_csv.exists():
            raise FileNotFoundError(
                f"{self.input_csv} not found — see README.md for the manual "
                "download step required before running this flow."
            )

        logger.info("Running rasterization")
        process = self.run_tasks(self.process, [[field] for field in self.field_list])
        self.log_run(process)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def air_pollution(config: AirPollutionConfiguration):
        AirPollution(config).run(config.run)


if __name__ == "__main__":
    config = get_config(AirPollutionConfiguration)
    AirPollution(config).run(config.run)
