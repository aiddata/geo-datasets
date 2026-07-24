"""
MODIS Land Surface Temperature MOD11C3 v061 (LP DAAC LPCLOUD)

Downloads monthly 0.05-degree CMG HDF4 files from LP DAAC via CMR search,
extracts day/night LST layers, and aggregates to yearly means.

Data source: https://www.earthdata.nasa.gov/data/catalog/lpcloud-mod11c3-061
CMR concept:  C2565791021-LPCLOUD  (verify: https://cmr.earthdata.nasa.gov/search/collections.json?short_name=MOD11C3&version=061&provider=LPCLOUD)
"""

import os
import shutil
import warnings
from pathlib import Path
from typing import Optional, Union

import numpy as np
import rasterio
import requests
from affine import Affine
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from pyhdf.SD import SD, SDC


CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
CONCEPT_ID = "C2565788897-LPCLOUD"


def export_raster(data, path, meta, **kwargs):
    """Export raster array to geotiff."""
    if not isinstance(meta, dict):
        raise ValueError("meta must be a dictionary")

    if "dtype" in meta:
        if meta["dtype"] != data.dtype:
            warnings.warn(
                f"Dtype specified by meta({meta['dtype']}) does not match data dtype ({data.dtype}). Adjusting data dtype to match meta."
            )
        data = data.astype(meta["dtype"])
    else:
        meta["dtype"] = data.dtype

    default_meta = {
        "count": 1,
        "crs": {"init": "epsg:4326"},
        "driver": "COG",
        "compress": "lzw",
        "nodata": -9999,
    }

    for k, v in default_meta.items():
        if k not in meta:
            if "quiet" not in kwargs or kwargs["quiet"] == False:
                print(
                    f"Value for `{k}` not in meta provided. Using default value ({v})"
                )
            meta[k] = v

    with rasterio.open(path, "w", **meta) as dst:
        dst.write(data)


def aggregate_rasters(file_list, method="mean"):
    """
    Aggregate multiple rasters with same features (dimensions, transform,
    pixel size, etc.) and creates single layer using aggregation method
    specified.

    Supported methods: mean (default), max, min, sum
    """
    store = None
    for ix, file_path in enumerate(file_list):
        try:
            raster = rasterio.open(file_path)
        except:
            print("Could not include file in aggregation ({0})".format(file_path))
            continue

        active = raster.read(masked=True)

        if store is None:
            store = active.copy()
        else:
            if active.shape != store.shape:
                raise Exception("Dimensions of rasters do not match")

            if method == "max":
                store = np.ma.array((store, active)).max(axis=0)
            elif method == "mean":
                if ix == 1:
                    weights = (~store.mask).astype(int)
                store = np.ma.average(
                    np.ma.array((store, active)),
                    axis=0,
                    weights=[weights, (~active.mask).astype(int)],
                )
                weights += (~active.mask).astype(int)
            elif method == "min":
                store = np.ma.array((store, active)).min(axis=0)
            elif method == "sum":
                store = np.ma.array((store, active)).sum(axis=0)
            else:
                raise Exception("Invalid method")

    store = store.filled(raster.nodata)
    return store, raster.meta


class MODISLandSurfaceTempConfiguration(BaseDatasetConfiguration):
    process_dir: str
    raw_dir: str
    output_dir: str
    # NASA Earthdata Login bearer token — stored in gitignored .env, not committed.
    earthdata_token: str
    # Comma-separated years (e.g. "2000,2001"). String, not list, so the
    # Prefect run form renders a text input rather than the array widget,
    # whose "add item" button submits the form.
    years: str
    overwrite_download: bool
    overwrite_monthly: bool
    overwrite_yearly: bool


class MODISLandSurfaceTemp(Dataset):
    name = "MODIS Land Surface Temperatures"

    def __init__(self, config: MODISLandSurfaceTempConfiguration):
        self.auth_headers = {"Authorization": f"Bearer {config.earthdata_token}"}

        self.years = [int(v.strip()) for v in config.years.split(",") if v.strip()]

        self.overwrite_download = config.overwrite_download
        self.overwrite_monthly = config.overwrite_monthly
        self.overwrite_yearly = config.overwrite_yearly

        self.process_dir = Path(config.process_dir)
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.method = "mean"

    # ── CMR search ──────────────────────────────────────────────────────────

    def test_connection(self):
        logger = self.get_logger()
        logger.info("Testing connection...")
        resp = requests.get(
            CMR_GRANULES_URL,
            params={"concept_id": CONCEPT_ID, "page_size": 1},
            headers=self.auth_headers,
            timeout=30,
        )
        resp.raise_for_status()
        logger.info("Connection successful")

    def search_granules(self, year: int) -> list[dict]:
        """Return all CMR granule entries for a given year."""
        logger = self.get_logger()
        results = []
        page = 1
        while True:
            resp = requests.get(
                CMR_GRANULES_URL,
                params={
                    "concept_id": CONCEPT_ID,
                    "temporal[]": f"{year}-01-01T00:00:00Z,{year}-12-31T23:59:59Z",
                    "page_size": 2000,
                    "page_num": page,
                },
                headers=self.auth_headers,
                timeout=60,
            )
            resp.raise_for_status()
            hits = resp.json()["feed"]["entry"]
            if not hits:
                break
            results.extend(hits)
            page += 1
        logger.info(f"Found {len(results)} granules for {year}")
        return results

    @staticmethod
    def granule_download_url(granule: dict) -> Optional[str]:
        """Extract the HTTPS .hdf download URL from a CMR granule entry."""
        for link in granule.get("links", []):
            href = link.get("href", "")
            if (
                href.startswith("https://")
                and href.endswith(".hdf")
                and link.get("rel", "").endswith("data#")
            ):
                return href
        return None

    # ── Download ─────────────────────────────────────────────────────────────

    def build_download_list(self) -> list[tuple]:
        logger = self.get_logger()
        logger.info("Preparing data download")
        tasks = []
        for year in self.years:
            granules = self.search_granules(year)
            for g in granules:
                url = self.granule_download_url(g)
                if url is None:
                    continue
                # time_start = "2000-03-01T00:00:00.000Z" → temporal = "200003"
                time_start = g.get("time_start", "")
                temporal = time_start[:7].replace("-", "")
                hdf_name = Path(url).name
                dst_path = self.raw_dir / f"{temporal}_{hdf_name}"
                tmp_path = self.process_dir / f"{temporal}_{hdf_name}"
                tasks.append((url, tmp_path, dst_path))
        return tasks

    def download_file(self, url: str, tmp_file: Path, dst_file: Path):
        logger = self.get_logger()
        dst_file, tmp_file = Path(dst_file), Path(tmp_file)
        self.process_dir.mkdir(parents=True, exist_ok=True)

        if dst_file.exists() and not self.overwrite_download:
            logger.info(f"File already exists: {dst_file}. Skipping...")
            return

        tmp_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with requests.get(url, headers=self.auth_headers, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(tmp_file, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            logger.info(f"Downloaded to tmp: {url} > {tmp_file}")
            shutil.copyfile(tmp_file, dst_file)
            logger.info(f"Copied to dst: {tmp_file} > {dst_file}")
        except Exception:
            tmp_file.unlink(missing_ok=True)
            raise

    # ── Processing (unchanged) ────────────────────────────────────────────────

    def build_process_list(self):
        flist = []

        for l_time, c_time in [("day", "Day"), ("night", "Night")]:
            layer = f"LST_{c_time}_CMG"
            (self.output_dir / "monthly" / l_time).mkdir(parents=True, exist_ok=True)

            for p in self.raw_dir.iterdir():
                if p.suffix == ".hdf":
                    temporal = p.name.split("_")[0]
                    output_path = (
                        self.output_dir
                        / "monthly"
                        / l_time
                        / f"modis_lst_{l_time}_cmg_{temporal}.tif"
                    )
                    tmp_path = (
                        self.process_dir / f"modis_lst_{l_time}_cmg_{temporal}.tif"
                    )

                    flist.append([p, layer, tmp_path, output_path])

        return flist

    def process_hdf(self, input_path: Union[str, Path], layer, tmp_path, output_path):
        logger = self.get_logger()
        self.process_dir.mkdir(parents=True, exist_ok=True)

        # pyhdf doesn't accept pathlib.Path objects
        if isinstance(input_path, Path):
            input_path = input_path.as_posix()

        if self.overwrite_monthly or not os.path.isfile(output_path):
            file = SD(input_path, SDC.READ)
            img = file.select(layer)
            data = img.get() * img.attributes()["scale_factor"]

            # 5600m / 0.05 degree resolution, global coverage
            transform = Affine(0.05, 0, -180, 0, -0.05, 90)
            meta = {
                "transform": transform,
                "nodata": 0,
                "height": data.shape[0],
                "width": data.shape[1],
            }
            export_raster(np.array([data]), tmp_path, meta, quiet=True)

            logger.info(f"Processed to tmp: {input_path} > {tmp_path}")
            shutil.copyfile(tmp_path, output_path)
            logger.info(f"Copied to dst: {tmp_path} > {output_path}")

        else:
            logger.info(f"{output_path} already exists, skipping...")

    def build_aggregation_list(self):
        src_dir = self.output_dir / "monthly"

        dst_dir = self.output_dir / "yearly"
        dst_dir.mkdir(parents=True, exist_ok=True)

        flist = []
        data_class_list = ["day", "night"]

        for data_class in data_class_list:
            month_files = [
                c for c in (src_dir / data_class).iterdir() if c.suffix == ".tif"
            ]
            year_months = {}

            for mfile in month_files:
                myear = mfile.name.split("_")[-1][:4]
                if myear not in year_months:
                    year_months[myear] = list()
                year_months[myear].append(mfile.as_posix())

            for year_group, month_paths in year_months.items():
                (dst_dir / data_class / self.method).mkdir(parents=True, exist_ok=True)
                output_path = (
                    dst_dir
                    / data_class
                    / self.method
                    / f"modis_lst_{data_class}_cmg_{year_group}.tif"
                )
                tmp_path = (
                    self.process_dir
                    / f"{self.method}_modis_lst_{data_class}_cmg_{year_group}.tif"
                )

                flist.append(
                    (year_group, self.method, month_paths, tmp_path, output_path)
                )

        return flist

    def run_yearly_data(self, year, method, year_files, tmp_path, out_path):
        logger = self.get_logger()
        self.process_dir.mkdir(parents=True, exist_ok=True)

        if not os.path.isfile(out_path) or self.overwrite_yearly:
            data, meta = aggregate_rasters(file_list=year_files, method=method)
            export_raster(data, tmp_path, meta)

            logger.info(f"Processed to tmp: {year}_{method} > {tmp_path}")
            shutil.copyfile(tmp_path, out_path)
            logger.info(f"Copied to dst: {tmp_path} > {out_path}")

        else:
            logger.info(f"{out_path} already exists, skipping...")

    def main(self):
        self.test_connection()

        download_list = self.build_download_list()
        download = self.run_tasks(self.download_file, download_list)
        self.log_run(download)

        process_list = self.build_process_list()
        process = self.run_tasks(self.process_hdf, process_list)
        self.log_run(process)

        data_to_agg = self.build_aggregation_list()
        agg = self.run_tasks(self.run_yearly_data, data_to_agg)
        self.log_run(agg)


try:
    from prefect import flow
except Exception:
    pass
else:
    @flow
    def modis_lst(config: MODISLandSurfaceTempConfiguration):
        MODISLandSurfaceTemp(config).run(config.run)


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()
    config = get_config(MODISLandSurfaceTempConfiguration)
    MODISLandSurfaceTemp(config).run(config.run)
