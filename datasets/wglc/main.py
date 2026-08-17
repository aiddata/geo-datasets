"""
WWLLN Global Lightning Climatology (WGLC) - monthly lightning stroke density

https://zenodo.org/records/20277101
https://wwlln.net/

Downloads the 5 arc-minute monthly timeseries NetCDF (density only - the
5-arcmin release only includes density; power statistics are only available
at 30 arc-minute resolution) and extracts each monthly band to a COG.
"""

import hashlib
from pathlib import Path

import netCDF4 as nc
import numpy as np
import rasterio
import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config

OUTPUT_CRS = "EPSG:4326"
VARIABLE = "density"


class WGLCConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    download_url: str
    # Zenodo's file URLs end in "/content", so the real filename can't be
    # derived from the URL and must be given explicitly.
    download_filename: str
    # Zenodo-published MD5 of the source file; verified after download.
    expected_md5: str
    # How to aggregate a year's 12 monthly bands: "sum" (default) totals the
    # monthly density values as a proxy for annual lightning activity;
    # "mean"/"max"/"min" are also supported.
    year_agg_method: str
    overwrite_download: bool
    overwrite_processing: bool


class WGLC(Dataset):
    name = "WWLLN Global Lightning Climatology (Density)"

    def __init__(self, config: WGLCConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.monthly_dir = self.output_dir / "monthly"
        self.yearly_dir = self.output_dir / "yearly"
        self.download_url = config.download_url
        self.expected_md5 = config.expected_md5
        self.year_agg_method = config.year_agg_method
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

        self.download_path = self.raw_dir / config.download_filename

    def download(self):
        """Download the source NetCDF and verify it against the published MD5."""
        logger = self.get_logger()

        if self.download_path.exists() and not self.overwrite_download:
            logger.info(f"Download exists, skipping: {self.download_path}")
            return

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        with self.tmp_to_dst_file(self.download_path) as tmp_path:
            logger.info(f"Downloading {self.download_url}")
            md5 = hashlib.md5()
            with requests.get(self.download_url, stream=True) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                        md5.update(chunk)

            if md5.hexdigest() != self.expected_md5:
                raise ValueError(
                    f"MD5 mismatch for {self.download_url}: "
                    f"expected {self.expected_md5}, got {md5.hexdigest()}"
                )
        logger.info(f"Downloaded and verified {self.download_path}")

    def build_process_list(self):
        """Build a (band, year, month) task per monthly band in the source file."""
        ds = nc.Dataset(self.download_path)
        try:
            times = nc.num2date(ds.variables["time"][:], ds.variables["time"].units)
        finally:
            ds.close()

        return [(band, t.year, t.month) for band, t in enumerate(times, start=1)]

    def process_band(self, band: int, year: int, month: int):
        """Extract one monthly band from the source NetCDF to a COG."""
        logger = self.get_logger()

        output_path = self.monthly_dir / f"wglc_density_{year}_{month:02d}.tif"
        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Output exists, skipping: {output_path}")
            return

        subdataset = f'NETCDF:"{self.download_path.as_posix()}":{VARIABLE}'
        with rasterio.open(subdataset) as src:
            data = src.read(band)
            meta = src.meta.copy()
            meta.update(
                count=1,
                driver="COG",
                compress="LZW",
                crs=OUTPUT_CRS,
                nodata=src.nodata,
            )

            with self.tmp_to_dst_file(
                output_path, make_dst_dir=True, validate_cog=True
            ) as tmp_dst:
                with rasterio.open(tmp_dst, "w", **meta) as dst:
                    dst.write(data, 1)
        logger.info(f"Saved {output_path}")

    def build_year_tasks(self):
        """Group monthly COGs by year; only years with a complete 12 months."""
        year_months = {}
        for f in self.monthly_dir.glob("wglc_density_*.tif"):
            year = f.stem.split("_")[2]
            year_months.setdefault(year, []).append(f)
        return [
            (year, sorted(files))
            for year, files in year_months.items()
            if len(files) == 12
        ]

    def aggregate_rasters(self, file_list):
        """Aggregate same-shape rasters with self.year_agg_method, respecting nodata."""
        stack = None
        profile = None
        for i, path in enumerate(sorted(file_list)):
            with rasterio.open(path) as src:
                data = src.read(1, masked=True)
                if profile is None:
                    profile = src.profile
                if stack is None:
                    stack = np.ma.empty((len(file_list), *data.shape), dtype=data.dtype)
                stack[i] = data

        if self.year_agg_method == "sum":
            result = stack.sum(axis=0)
        elif self.year_agg_method == "mean":
            result = stack.mean(axis=0)
        elif self.year_agg_method == "max":
            result = stack.max(axis=0)
        elif self.year_agg_method == "min":
            result = stack.min(axis=0)
        else:
            raise ValueError(f"Unsupported year_agg_method: {self.year_agg_method!r}")

        return result.filled(profile["nodata"]), profile

    def process_year(self, year: str, month_files: list):
        """Aggregate a year's monthly COGs into a single annual COG."""
        logger = self.get_logger()

        output_path = self.yearly_dir / f"wglc_density_{year}.tif"
        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Output exists, skipping: {output_path}")
            return

        data, profile = self.aggregate_rasters(month_files)
        # profile comes from a monthly COG's own tiling metadata (blockxsize/
        # blockysize/tiled/interleave), which the COG driver computes itself
        # and warns about if passed back in as creation options
        for key in ("blockxsize", "blockysize", "tiled", "interleave"):
            profile.pop(key, None)
        profile.update(driver="COG", compress="LZW")

        with self.tmp_to_dst_file(
            output_path, make_dst_dir=True, validate_cog=True
        ) as tmp_dst:
            with rasterio.open(tmp_dst, "w", **profile) as dst:
                dst.write(data, 1)
        logger.info(f"Saved {output_path}")

    def main(self):
        logger = self.get_logger()

        logger.info("Downloading source data")
        self.download()

        logger.info("Building band list")
        process_list = self.build_process_list()

        logger.info(f"Processing {len(process_list)} monthly bands")
        results = self.run_tasks(self.process_band, process_list)
        self.log_run(results)

        logger.info("Building year list")
        year_tasks = self.build_year_tasks()

        logger.info(f"Aggregating {len(year_tasks)} years")
        year_results = self.run_tasks(self.process_year, year_tasks)
        self.log_run(year_results)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def wglc(config: WGLCConfiguration):
        WGLC(config).run(config.run)


if __name__ == "__main__":
    config = get_config(WGLCConfiguration)
    WGLC(config).run(config.run)
