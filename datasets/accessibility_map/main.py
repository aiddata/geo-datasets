"""
JRC Global Accessibility Map (GAM) — travel time to major cities

https://forobs.jrc.ec.europa.eu/gam

Estimated travel time (in minutes) to the nearest city of 50,000 or more people
in the year 2000 (Nelson, 2008). A single static global raster. This flow
downloads the source archive and rewrites the packaged GeoTIFF as a validated
Cloud Optimized GeoTIFF.
"""
from pathlib import Path

import rasterio
import requests
from pydantic import field_validator

from data_manager import BaseDatasetConfiguration, Dataset, get_config

DOWNLOAD_URL = "https://forobs.jrc.ec.europa.eu/data/products/gam/access_50k.zip"
# name of the GeoTIFF packaged inside the archive
ARCHIVE_TIF = "acc_50k.tif"


class AccessibilityMapConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_process: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class AccessibilityMap(Dataset):

    name = "Accessibility Map"

    def __init__(self, config: AccessibilityMapConfiguration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_download = config.overwrite_download
        self.overwrite_process = config.overwrite_process

        self.download_path = self.raw_dir / "access_50k.zip"
        self.output_path = self.output_dir / "access_50k.tif"

    def download(self):
        logger = self.get_logger()

        if not self.overwrite_download and self.download_path.exists():
            logger.info(f"Download exists, skipping: {self.download_path}")
            return

        logger.info(f"Downloading {DOWNLOAD_URL} to {self.download_path}")
        with self.tmp_to_dst_file(self.download_path, make_dst_dir=True) as tmp:
            with requests.get(DOWNLOAD_URL, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(tmp, "wb") as dst:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        dst.write(chunk)
        logger.info(f"Downloaded {self.download_path}")

    def process(self):
        logger = self.get_logger()

        if not self.overwrite_process and self.output_path.exists():
            logger.info(f"Output exists, skipping: {self.output_path}")
            return

        # read the packaged GeoTIFF straight out of the zip and rewrite as a COG
        vsi_path = f"/vsizip/{self.download_path}/{ARCHIVE_TIF}"
        logger.info(f"Converting {vsi_path} to COG {self.output_path}")

        with rasterio.open(vsi_path) as src:
            meta = src.meta.copy()
            meta.update(driver="COG", compress="LZW")
            with self.tmp_to_dst_file(
                self.output_path, make_dst_dir=True, validate_cog=True
            ) as tmp:
                with rasterio.open(tmp, "w", **meta) as dst:
                    dst.write(src.read())

        logger.info(f"Wrote {self.output_path}")

    def main(self):
        logger = self.get_logger()
        logger.info("Running accessibility map download")
        self.download()
        self.process()


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def accessibility_map(config: AccessibilityMapConfiguration):
        AccessibilityMap(config).run(config.run)


if __name__ == "__main__":
    config = get_config(AccessibilityMapConfiguration)
    AccessibilityMap(config).run(config.run)
