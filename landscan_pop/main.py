"""

1. Set the raw and output data directories in the config
2. Go to https://landscan.ornl.gov and manually download all desired years of the "LandScan Global" population dataset
3. Make sure all files are downloaded to a folder named "compressed" within the raw_data directory path specified in your config
4. Edit the years in the config if needed (if you only downloaded a subset of years, or only want to extract/process a subset of years)


"""

import os
import zipfile
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import List

import rasterio
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class LandScanPopConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    years: List[int]
    run_extract: bool
    run_conversion: bool
    overwrite_extract: bool
    overwrite_conversion: bool


class LandScanPop(Dataset):
    name = "LandScan Population"

    def __init__(self, config: LandScanPopConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)

        self.years = config.years

        self.run_extract = config.run_extract
        self.run_conversion = config.run_conversion

        self.overwrite_extract = config.overwrite_extract
        self.overwrite_conversion = config.overwrite_conversion

        self.download_dir = self.raw_dir / "compressed"
        os.makedirs(self.download_dir, exist_ok=True)

        self.extract_dir = self.raw_dir / "uncompressed"
        os.makedirs(self.extract_dir, exist_ok=True)

        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def unzip_file(self, zip_file, out_dir):
        """Extract a zipfile"""
        logger = self.get_logger()
        if os.path.isdir(out_dir) and not self.overwrite_extract:
            logger.info(f"Extracted directory exists - skipping ({out_dir})")
        else:
            logger.info(f"Extracting {zip_file} to {out_dir}")
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(out_dir)

    def convert_to_cog(self, src, final_dst):
        """Convert a raster from ESRI grid format to COG format"""
        logger = self.get_logger()

        if os.path.isfile(final_dst) and not self.overwrite_conversion:
            logger.info(f"COG exists - skipping ({final_dst})")
        else:
            logger.info(f"Converting to COG ({final_dst})")
            with rasterio.open(src) as src:
                assert len(set(src.block_shapes)) == 1
                meta = src.meta.copy()
                meta.update(
                    {
                        "driver": "COG",
                        "compress": "LZW",
                    }
                )

                with self.tmp_to_dst_file(final_dst, validate_cog=True) as tmp_dst:
                    with rasterio.open(tmp_dst, "w", **meta) as dst:
                        for ji, window in src.block_windows(1):
                            in_data = src.read(window=window)
                            dst.write(in_data, window=window)

    def build_extract_list(self):
        """Build a list of files to extract"""
        flist = []
        for x in self.download_dir.iterdir():
            y = int(x.name.split("-")[2])
            if x.name.endswith(".zip") and y in self.years:
                flist.append((self.download_dir / x, self.extract_dir / x.name[:-4]))

        return flist

    def build_conversion_list(self):
        """Build a list of files to convert"""
        flist = []
        for x in self.extract_dir.iterdir():
            y = int(x.name.split("-")[2])
            if os.path.isdir(x) and y in self.years:
                fname = x.name.replace("-assets", ".tif")
                flist.append((x / fname, self.output_dir / fname))

        return flist

    def main(self):
        logger = self.get_logger()

        logger.info("Starting pipeline...")

        # unzip
        if self.run_extract:
            logger.info("Running extract tasks...")
            ex_list = self.build_extract_list()
            extract = self.run_tasks(self.unzip_file, ex_list)
            self.log_run(extract)

        # convert from esri grid format to COG
        if self.run_conversion:
            logger.info("Running conversion tasks...")
            conv_list = self.build_conversion_list()
            conv = self.run_tasks(self.convert_to_cog, conv_list)
            self.log_run(conv)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def landscan_pop(config: LandScanPopConfiguration):
        LandScanPop(config).run(config.run)


if __name__ == "__main__":
    config = get_config(LandScanPopConfiguration)
    LandScanPop(config).run(config.run)
