import os
import shutil
import zipfile
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import List, Literal

import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config

# The raster's own filename isn't assumed stable across releases either —
# the only thing assumed is that it's the sole .tif in a "01_Data" dir.
TARGET_DATA_DIR = "01_Data"


class CRHABConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    download_url: str
    max_retries: int
    overwrite_download: bool
    overwrite_output: bool


class CRHAB(Dataset):

    name = "Critical Habitats"

    def __init__(self, config: CRHABConfiguration):

        self.download_url = config.download_url
        self.download_path = Path(config.download_url)

        self.raw_dir = Path(config.raw_dir)
        self.zip_path = self.raw_dir / self.download_path.name
        # The path to the raster inside the zip isn't assumed here — it's
        # not safe to guess based on the zip filename or prior runs, since
        # the publisher's internal folder structure can change between
        # releases. extract_data() inspects the zip's actual namelist and
        # sets this once it knows the real path.
        self.data_path = None
        self.output_dir = Path(config.output_dir)
        self.output_path = self.output_dir / "critical_habitats.tif"

        self.max_retries = config.max_retries

        self.overwrite_download = config.overwrite_download
        self.overwrite_output = config.overwrite_output

    def download_data(self):
        """
        Download data zip from source
        """
        logger = self.get_logger()

        if self.zip_path.exists() and not self.overwrite_download:
            logger.info(f"Download Exists: {self.zip_path}")
            return

        attempts = 1
        while attempts <= self.max_retries:
            try:
                with requests.get(self.download_url, stream=True, verify=True) as r:
                    r.raise_for_status()
                    with open(self.zip_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            f.write(chunk)
                logger.info(f"Downloaded: {self.download_url}")
                return (self.download_url, self.zip_path)
            except Exception as e:
                attempts += 1
                if attempts > self.max_retries:
                    logger.info(
                        f"{str(e)}: Failed to download: {str(self.download_url)}"
                    )
                    logger.exception(e)
                    raise
                else:
                    logger.info(f"Attempt {str(attempts)} : {str(self.download_url)}")

    def find_target_member(self, zip_ref):
        """Locate the target raster inside the zip.

        Neither the raster's filename nor its full path is assumed stable
        across releases or matched to the zip's own filename — the only
        assumption is that there's exactly one .tif directly inside a
        "01_Data" directory somewhere in the zip. Read from the zip's
        actual namelist every time rather than a hardcoded path.
        """
        matches = [
            name
            for name in zip_ref.namelist()
            if Path(name).parent.name == TARGET_DATA_DIR
            and Path(name).suffix.lower() == ".tif"
        ]
        if len(matches) == 0:
            raise Exception(
                f"No .tif found in a {TARGET_DATA_DIR} directory in {self.zip_path}"
            )
        if len(matches) > 1:
            raise Exception(
                f"Expected exactly one .tif in a {TARGET_DATA_DIR} directory in "
                f"{self.zip_path}, found {len(matches)}: {matches}"
            )
        return matches[0]

    def extract_data(self):
        """Extract data from downloaded zip file"""

        logger = self.get_logger()

        if not self.zip_path.exists():
            logger.info(f"Error: Data download not found: {self.zip_path}")
            raise Exception(f"Data file not found: {self.zip_path}")

        with zipfile.ZipFile(self.zip_path, "r") as zip_ref:
            target_member = self.find_target_member(zip_ref)
            self.data_path = self.raw_dir / target_member

            if self.data_path.exists() and not self.overwrite_download:
                logger.info(f"Extract Exists: {self.data_path}")
                return

            logger.info(f"Extracting: {self.zip_path}")
            zip_ref.extractall(self.raw_dir)

    def process_data(self):
        """Copy extract file to output"""
        logger = self.get_logger()

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.output_path.exists() and not self.overwrite_output:
            logger.info(f"Output Exists: {self.output_path}")
            return
        else:
            logger.info(f"Processing: {self.data_path}")
            shutil.copy(self.data_path, self.output_path)

    def main(self):

        logger = self.get_logger()

        logger.info("Running data download")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.download_data()

        logger.info("Extracting Data")
        self.extract_data()

        logger.info("Processing Data")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.process_data()


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def critical_habitats(config: CRHABConfiguration):
        CRHAB(config).run(config.run)


if __name__ == "__main__":
    config = get_config(CRHABConfiguration)
    CRHAB(config).run(config.run)
