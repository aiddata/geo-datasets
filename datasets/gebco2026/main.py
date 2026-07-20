"""
GEBCO_2026 Grid — global elevation/bathymetry and derived slope

https://www.gebco.net/data-products-gridded-bathymetry-data/gebco2026-grid

Downloads the GEBCO_2026 ice surface elevation grid (distributed as 8
90x90 degree quadrant GeoTIFFs in a single zip), mosaics them into one
seamless global elevation raster, and computes a global slope raster
(degrees) from it using Horn's method with a latitude-dependent correction
for the varying real-world width of a degree of longitude.
"""
import zipfile
from pathlib import Path

import numpy as np
import rasterio
import requests
from pydantic import field_validator
from rasterio.merge import merge
from rasterio.windows import Window

from data_manager import BaseDatasetConfiguration, Dataset, get_config

DOWNLOAD_URL = (
    "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/"
    "ice_surface_elevation/geotiff/gebco_2026_geotiff.zip?download=1"
)
ZIP_NAME = "gebco_2026_geotiff.zip"
METERS_PER_DEGREE = 111320


class Gebco2026Configuration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_elevation: bool
    overwrite_slope: bool

    @field_validator("raw_dir", "output_dir")
    @classmethod
    def validate_path(cls, f: str) -> Path:
        return Path(f)


class Gebco2026(Dataset):

    name = "GEBCO 2026"

    def __init__(self, config: Gebco2026Configuration):
        self.config = config
        self.raw_dir = config.raw_dir
        self.output_dir = config.output_dir
        self.overwrite_download = config.overwrite_download
        self.overwrite_elevation = config.overwrite_elevation
        self.overwrite_slope = config.overwrite_slope

        self.download_path = self.raw_dir / ZIP_NAME
        self.elevation_path = self.output_dir / "gebco2026_elevation.tif"
        self.slope_path = self.output_dir / "gebco2026_slope.tif"

    def download(self):
        logger = self.get_logger()

        if not self.overwrite_download and self.download_path.exists():
            logger.info(f"Download exists, skipping: {self.download_path}")
            return

        logger.info(f"Downloading {DOWNLOAD_URL} to {self.download_path}")
        with self.tmp_to_dst_file(
            self.download_path, make_dst_dir=True, tmp_dir=self.raw_dir
        ) as tmp:
            with requests.get(DOWNLOAD_URL, stream=True, timeout=120) as response:
                response.raise_for_status()
                with open(tmp, "wb") as dst:
                    for chunk in response.iter_content(chunk_size=1024 * 1024 * 8):
                        dst.write(chunk)
        logger.info(f"Downloaded {self.download_path}")

    def list_tiles(self):
        """Return vsizip paths for the 8 quadrant tile GeoTIFFs in the zip."""
        with zipfile.ZipFile(self.download_path) as zf:
            names = [n for n in zf.namelist() if n.endswith(".tif")]
        if len(names) != 8:
            raise ValueError(f"Expected 8 quadrant tiles, found {len(names)}")
        return [f"/vsizip/{{{self.download_path}}}/{name}" for name in names]

    def build_elevation_mosaic(self):
        """Mosaic the 8 quadrant tiles into a single global elevation COG.

        Uses rasterio.merge, which reads each tile's own georeferencing to
        place it in the output — rather than us parsing tile bounds out of
        filenames — and writes to disk in windows rather than requiring the
        full ~7GB global array in memory.

        Returns the global affine/shape/dtype/nodata so build_slope() doesn't
        need to re-derive them.
        """
        logger = self.get_logger()

        tiles = self.list_tiles()

        with rasterio.open(tiles[0]) as sample:
            dtype = sample.dtypes[0]
            nodata = sample.nodata

        if not self.overwrite_elevation and self.elevation_path.exists():
            logger.info(f"Elevation raster exists, skipping: {self.elevation_path}")
        else:
            dst_kwds = {
                "driver": "COG",
                "compress": "LZW",
                "dtype": dtype,
                "nodata": nodata,
            }

            logger.info(f"Mosaicking {len(tiles)} tiles to {self.elevation_path}")
            with self.tmp_to_dst_file(
                self.elevation_path, make_dst_dir=True, validate_cog=True
            ) as tmp:
                merge(tiles, dst_path=tmp, dst_kwds=dst_kwds)
            logger.info(f"Wrote {self.elevation_path}")

        with rasterio.open(self.elevation_path) as written:
            return written.transform, (written.height, written.width), dtype, nodata

    def build_slope(self, affine, shape, elevation_dtype, elevation_nodata):
        logger = self.get_logger()

        if not self.overwrite_slope and self.slope_path.exists():
            logger.info(f"Slope raster exists, skipping: {self.slope_path}")
            return

        height, width = shape
        pixel_size_deg = affine.a
        ymax = affine.f
        slope_nodata = -1.0
        dy = METERS_PER_DEGREE * pixel_size_deg

        meta = {
            "count": 1,
            "crs": "EPSG:4326",
            "dtype": "float32",
            "transform": affine,
            "driver": "COG",
            "compress": "LZW",
            "height": height,
            "width": width,
            "nodata": slope_nodata,
        }

        # process the global raster in horizontal strips (with a 1-row halo
        # above/below) rather than loading the whole ~7GB+ array into memory
        strip_height = 2000

        logger.info(f"Writing slope raster to {self.slope_path}")
        with self.tmp_to_dst_file(
            self.slope_path, make_dst_dir=True, validate_cog=True
        ) as tmp:
            with rasterio.open(self.elevation_path) as src, rasterio.open(
                tmp, "w", **meta
            ) as dst:
                for row_off in range(0, height, strip_height):
                    rows = min(strip_height, height - row_off)

                    halo_top = 1 if row_off > 0 else 0
                    halo_bottom = 1 if row_off + rows < height else 0
                    read_window = Window(
                        0,
                        row_off - halo_top,
                        width,
                        rows + halo_top + halo_bottom,
                    )
                    strip = src.read(1, window=read_window).astype("float64")

                    # pad top/bottom with edge values where there's no halo
                    # (raster edge), and always pad left/right with edge values
                    pad_top = 1 - halo_top
                    pad_bottom = 1 - halo_bottom
                    padded = np.pad(
                        strip, ((pad_top, pad_bottom), (1, 1)), mode="edge"
                    )

                    row_lat = ymax - (np.arange(row_off, row_off + rows) + 0.5) * pixel_size_deg
                    dx = METERS_PER_DEGREE * pixel_size_deg * np.cos(np.radians(row_lat))

                    z1 = padded[:-2, :-2]
                    z2 = padded[:-2, 1:-1]
                    z3 = padded[:-2, 2:]
                    z4 = padded[1:-1, :-2]
                    z6 = padded[1:-1, 2:]
                    z7 = padded[2:, :-2]
                    z8 = padded[2:, 1:-1]
                    z9 = padded[2:, 2:]

                    dzdx = ((z3 + 2 * z6 + z9) - (z1 + 2 * z4 + z7)) / (8 * dx)[:, None]
                    dzdy = ((z7 + 2 * z8 + z9) - (z1 + 2 * z2 + z3)) / (8 * dy)

                    slope = np.degrees(np.arctan(np.sqrt(dzdx**2 + dzdy**2)))

                    if elevation_nodata is not None:
                        # unpadded, halo-stripped elevation values for these
                        # output rows, to mask nodata cells in the result
                        orig_rows = strip[halo_top : halo_top + rows]
                        slope = np.where(orig_rows == elevation_nodata, slope_nodata, slope)

                    logger.info(f"Writing slope rows {row_off}-{row_off + rows}")
                    write_window = Window(0, row_off, width, rows)
                    dst.write(slope.astype("float32"), 1, window=write_window)

        logger.info(f"Wrote {self.slope_path}")

    def main(self):
        logger = self.get_logger()

        logger.info("Running GEBCO 2026 download")
        self.download()

        logger.info("Building global elevation mosaic")
        affine, shape, dtype, nodata = self.build_elevation_mosaic()

        logger.info("Building global slope raster")
        self.build_slope(affine, shape, dtype, nodata)


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def gebco2026(config: Gebco2026Configuration):
        Gebco2026(config).run(config.run)


if __name__ == "__main__":
    config = get_config(Gebco2026Configuration)
    Gebco2026(config).run(config.run)
