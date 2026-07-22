"""
MODIS Land Cover MCD12Q1 v061 (LP DAAC LPCLOUD)

Downloads yearly land cover HDF4 tiles from LP DAAC via CMR search, extracts
the IGBP classification layer (LC_Type1) from each tile, mosaics all tiles into
a single global raster per year, and reprojects to WGS84.

Data source: https://lpdaac.usgs.gov/products/mcd12q1v061/
CMR concept:  C2484079608-LPCLOUD
"""

import shutil
import subprocess
from pathlib import Path
from typing import Optional

import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config


CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
CONCEPT_ID = "C2484079608-LPCLOUD"
EOS_GRID_NAME = "MCD12Q1"


class MCD12Q1Configuration(BaseDatasetConfiguration):
    raw_dir: str
    process_dir: str
    output_dir: str
    # NASA Earthdata Login bearer token — stored in gitignored .env, not committed.
    earthdata_token: str
    # Comma-separated years, e.g. "2001,2002,2003". String so the Prefect run
    # form renders a text input rather than the array widget.
    years: str
    # HDF4 subdataset / field name to extract. LC_Type1 = IGBP classification.
    lc_type: str
    overwrite_download: bool
    overwrite_processing: bool
    overwrite_mosaic: bool


class MCD12Q1(Dataset):
    name = "MODIS Land Cover MCD12Q1 v061"

    def __init__(self, config: MCD12Q1Configuration):
        self.raw_dir = Path(config.raw_dir)
        self.process_dir = Path(config.process_dir)
        self.output_dir = Path(config.output_dir)
        self.auth_headers = {"Authorization": f"Bearer {config.earthdata_token}"}
        self.years = [int(y.strip()) for y in config.years.split(",") if y.strip()]
        self.lc_type = config.lc_type
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing
        self.overwrite_mosaic = config.overwrite_mosaic

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.process_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ── CMR search ──────────────────────────────────────────────────────────

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
        tasks = []
        for year in self.years:
            granules = self.search_granules(year)
            year_dir = self.raw_dir / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)
            for g in granules:
                url = self.granule_download_url(g)
                if url is None:
                    continue
                dst = year_dir / Path(url).name
                tasks.append((url, dst))
        return tasks

    def download_granule(self, url: str, dst: Path):
        logger = self.get_logger()
        dst = Path(dst)
        if dst.exists() and not self.overwrite_download:
            logger.info(f"Already downloaded: {dst.name}")
            return
        tmp = dst.with_suffix(".tmp.hdf")
        try:
            with requests.get(url, headers=self.auth_headers, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            shutil.move(str(tmp), dst)
            logger.info(f"Downloaded: {dst.name}")
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

    # ── Tile extraction ──────────────────────────────────────────────────────

    def build_process_list(self) -> list[tuple]:
        tasks = []
        for year in self.years:
            year_raw = self.raw_dir / str(year)
            tile_dir = self.process_dir / str(year) / "tiles"
            tile_dir.mkdir(parents=True, exist_ok=True)
            for hdf in sorted(year_raw.glob("*.hdf")):
                tile_tif = tile_dir / (hdf.stem + f"_{self.lc_type}.tif")
                tasks.append((hdf, tile_tif))
        return tasks

    def process_tile(self, hdf_path: Path, tile_tif: Path):
        """Extract land cover subdataset from one HDF4 tile to GeoTIFF."""
        logger = self.get_logger()
        hdf_path, tile_tif = Path(hdf_path), Path(tile_tif)
        if tile_tif.exists() and not self.overwrite_processing:
            logger.info(f"Tile already processed: {tile_tif.name}")
            return
        subdataset = f'HDF4_EOS:EOS_GRID:"{hdf_path}":{EOS_GRID_NAME}:{self.lc_type}'
        tmp = tile_tif.with_suffix(".tmp.tif")
        try:
            subprocess.run(
                [
                    "gdal_translate",
                    "-of", "GTiff",
                    "-co", "COMPRESS=LZW",
                    "-co", "TILED=YES",
                    subdataset,
                    str(tmp),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            shutil.move(str(tmp), tile_tif)
            logger.info(f"Extracted: {tile_tif.name}")
        except subprocess.CalledProcessError as e:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"gdal_translate failed for {hdf_path.name}: {e.stderr}") from e

    # ── Mosaic & reproject ───────────────────────────────────────────────────

    def build_mosaic_list(self) -> list[tuple]:
        tasks = []
        for year in self.years:
            tile_dir = self.process_dir / str(year) / "tiles"
            output_tif = self.output_dir / f"mcd12q1_061_{self.lc_type.lower()}_{year}.tif"
            tasks.append((tile_dir, output_tif))
        return tasks

    def mosaic_year(self, tile_dir: Path, output_tif: Path):
        """Mosaic all tiles for one year (SIN) and reproject to WGS84."""
        logger = self.get_logger()
        tile_dir, output_tif = Path(tile_dir), Path(output_tif)
        if output_tif.exists() and not self.overwrite_mosaic:
            logger.info(f"Mosaic already exists: {output_tif.name}")
            return

        tiles = sorted(tile_dir.glob("*.tif"))
        if not tiles:
            logger.warning(f"No tiles to mosaic in {tile_dir}")
            return

        tmp_sin = self.process_dir / (output_tif.stem + "_sin.tif")
        tmp_wgs84 = self.process_dir / (output_tif.stem + "_wgs84.tmp.tif")
        try:
            # Mosaic in native MODIS sinusoidal projection
            subprocess.run(
                [
                    "gdal_merge.py",
                    "-of", "GTiff",
                    "-co", "COMPRESS=LZW",
                    "-co", "TILED=YES",
                    "-co", "BIGTIFF=YES",
                    "-init", "255",
                    "-n", "255",
                    "-o", str(tmp_sin),
                ] + [str(t) for t in tiles],
                check=True,
                capture_output=True,
                text=True,
            )
            logger.info(f"Mosaicked {len(tiles)} tiles for {output_tif.stem}")

            # Reproject SIN → WGS84; nearest-neighbor preserves integer class values
            subprocess.run(
                [
                    "gdalwarp",
                    "-t_srs", "EPSG:4326",
                    "-r", "near",
                    "-ot", "Byte",
                    "-co", "COMPRESS=LZW",
                    "-co", "TILED=YES",
                    "-co", "BIGTIFF=YES",
                    "-srcnodata", "255",
                    "-dstnodata", "255",
                    str(tmp_sin),
                    str(tmp_wgs84),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            shutil.move(str(tmp_wgs84), output_tif)
            logger.info(f"Reprojected to WGS84: {output_tif.name}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"GDAL command failed: {e.stderr}") from e
        finally:
            tmp_sin.unlink(missing_ok=True)
            tmp_wgs84.unlink(missing_ok=True)

    # ── Orchestration ─────────────────────────────────────────────────────────

    def main(self):
        logger = self.get_logger()

        logger.info("=== Download ===")
        download_list = self.build_download_list()
        logger.info(f"Downloading {len(download_list)} granules")
        download = self.run_tasks(self.download_granule, download_list)
        self.log_run(download)

        logger.info("=== Tile extraction ===")
        process_list = self.build_process_list()
        logger.info(f"Extracting {len(process_list)} tiles")
        process = self.run_tasks(self.process_tile, process_list)
        self.log_run(process)

        logger.info("=== Mosaic & reproject ===")
        mosaic_list = self.build_mosaic_list()
        logger.info(f"Mosaicking {len(mosaic_list)} years")
        mosaic = self.run_tasks(self.mosaic_year, mosaic_list)
        self.log_run(mosaic)


try:
    from prefect import flow
except Exception:
    pass
else:
    @flow
    def modis_landcover(config: MCD12Q1Configuration):
        MCD12Q1(config).run(config.run)


if __name__ == "__main__":
    import os
    import dotenv
    dotenv.load_dotenv()
    config = get_config(MCD12Q1Configuration)
    config.earthdata_token = os.environ.get("earthdata_token", config.earthdata_token)
    MCD12Q1(config).run(config.run)
