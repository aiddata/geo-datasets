"""

1. Set the raw and output data directories in the config
2. Set landscan_email (and, if needed, landscan_primary_use/landscan_sector) in config.toml and enable run_download
3. Edit the years in the config if needed (if you only want to download/extract/process a subset of years)


"""

import os
import time
import zipfile
from pathlib import Path

import rasterio
import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config

# ORNL's download form (https://landscan.ornl.gov) submits usage info to this
# public API Gateway endpoint (no auth/cookie required), which responds with a
# short-lived presigned S3 URL for the requested product/year.
DOWNLOAD_REQUEST_URL = "https://hm52ct46i7.execute-api.us-east-1.amazonaws.com/prod/download"

# the download server occasionally returns request errors; this allows a few
# retries before giving up
DOWNLOAD_RETRIES = 5
DOWNLOAD_RETRY_DELAY = 5  # seconds


class LandScanPopConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    # Comma-separated years (e.g. "2000,2001"). String, not list, so the
    # Prefect run form renders a text input rather than the array widget,
    # whose "add item" button submits the form.
    years: str
    run_download: bool
    run_extract: bool
    run_conversion: bool
    overwrite_download: bool
    overwrite_extract: bool
    overwrite_conversion: bool
    # Usage info submitted with each download request, matching the fields on
    # ORNL's download form (https://landscan.ornl.gov). Not secret, but kept
    # configurable since email identifies the requester.
    landscan_email: str
    landscan_primary_use: str
    landscan_sector: str


class LandScanPop(Dataset):
    name = "LandScan Population"

    def __init__(self, config: LandScanPopConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)

        self.years = [int(v.strip()) for v in config.years.split(",") if v.strip()]

        self.run_download = config.run_download
        self.run_extract = config.run_extract
        self.run_conversion = config.run_conversion

        self.overwrite_download = config.overwrite_download
        self.overwrite_extract = config.overwrite_extract
        self.overwrite_conversion = config.overwrite_conversion

        self.landscan_email = config.landscan_email
        self.landscan_primary_use = config.landscan_primary_use
        self.landscan_sector = config.landscan_sector

        self.download_dir = self.raw_dir / "compressed"
        os.makedirs(self.download_dir, exist_ok=True)

        self.extract_dir = self.raw_dir / "uncompressed"
        os.makedirs(self.extract_dir, exist_ok=True)

        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_download_url(self, year):
        """Submit the usage-info form for a given year and return the presigned S3 URL"""
        response = requests.post(
            DOWNLOAD_REQUEST_URL,
            json={
                "email": self.landscan_email,
                "primary_use": self.landscan_primary_use,
                "sector": self.landscan_sector,
                "product": "global",
                "productModifier1": str(year),
            },
        )
        response.raise_for_status()
        # response is a bare presigned URL, not JSON - but some responses come
        # back quoted, so strip that if present
        return response.text.strip().strip('"')

    def download_file(self, year):
        logger = self.get_logger()

        # the request form doesn't tell us the resulting filename ahead of
        # time, so check for an already-downloaded file for this year by
        # pattern before spending a request on the form submission
        existing = list(self.download_dir.glob(f"landscan-global-{year}-*.zip"))
        if existing and not self.overwrite_download:
            logger.info(f"Download exists - skipping ({existing[0]})")
            return (year, existing[0])

        dl_link = self.get_download_url(year)
        # the presigned URL's path ends in the actual S3 object filename
        # (e.g. "landscan-global-2023-assets.zip") - use that as-is rather
        # than inventing a local filename
        filename = requests.utils.urlparse(dl_link).path.rsplit("/", 1)[-1]
        local_filename = self.download_dir / filename

        response = None
        attempts = 0
        while attempts < DOWNLOAD_RETRIES:
            try:
                response = requests.get(dl_link, stream=True)
                break
            except requests.exceptions.RequestException as e:
                attempts += 1
                logger.info(f"Request error downloading year {year} ({e}), retrying...")
                time.sleep(DOWNLOAD_RETRY_DELAY)
        if response is None:
            raise RuntimeError(f"Could not reach download server for year {year}")

        response.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
        response.close()
        logger.info(f"Downloaded {local_filename}")
        return (year, local_filename)

    def build_download_list(self):
        """Build a list of years to download"""
        return [(year,) for year in self.years]

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

        if self.run_download:
            logger.info("Running download tasks...")
            dl_list = self.build_download_list()
            download = self.run_tasks(self.download_file, dl_list)
            self.log_run(download)

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
