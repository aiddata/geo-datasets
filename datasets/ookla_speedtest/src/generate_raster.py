from src.helpers import (
    GRID_SIZE,
    NUM_BANDS,
    OUTPUT_RASTER_FILE,
    MAP_BOUNDS,
    BAND16_COLS,
    BAND32_COLS,
)
from src.transform_populate import create_band_array
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import rasterio
import geopandas as gpd
import numpy as np
import logging
import sys
import gc
from typing import Dict, List

# instantiating the logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def make_raster_profile(
    num_bands: int = NUM_BANDS,
    grid_size: int = GRID_SIZE,
    map_bounds: tuple[float, float, float, float] = MAP_BOUNDS,
) -> Dict[str, object]:
    """
    Creates the raster metadata profile for writing the GeoTIFF file.
    Profile includes CRS, transform, and dtype information.
    ---
    Args:
        grid_size (int): dimensions of the raster grid (width x height)
        num_bands (int): number of raster bands included in the raster
        map_bounds (tuple): bounds in projected coordinates
    Returns:
        Dict[str, object]: raster metadata profile
    """
    left, bottom, right, top = map_bounds
    transform = from_bounds(left, bottom, right, top, grid_size, grid_size)
    profile = {
        "driver": "GTiff",
        "count": num_bands,
        "dtype": "uint32",
        "crs": CRS.from_epsg(3857),
        "compress": "lzw",
        "transform": transform,
        "width": grid_size,
        "height": grid_size,
    }
    return profile


def write_multiband_raster_chunks(
    gdf: gpd.GeoDataFrame,
    profile: dict,
    output_path: str = OUTPUT_RASTER_FILE,
    band32_cols: List[str] = BAND32_COLS,
    band16_cols: List[str] = BAND16_COLS,
) -> None:
    """
    Writes a multiband GeoTIFF raster file from the GeoDataFrame. Completes this
    one band at a time then deletes in order to maximize computational expense.
    Each band is created from create_band_array(), converted to dense 2D array,
    flipped vertically for top-left origin, and written to the output raster file.
    ---
    Args:
        gdf (gpd.GeoDataFrame) is the input GeoDataFrame
        profile (dict) is the profile outputted from make_raster_profile()
        output_path (str) the target file path for saving the GeoTIFF
        band32_cols (List[str]) are the column names for the uint32 bands
        band16_cols (List[str]) are the column names for the uint16 bands
    Returns: None
    """
    total_bands = len(band32_cols) + len(band16_cols)
    profile = profile.copy()
    profile["count"] = total_bands
    profile["BIGTIFF"] = "YES"
    profile["dtype"] = "uint32"  # this is a placeholder

    # Writing the uint32 bands
    with rasterio.open(output_path, "w", **profile) as dst:
        for i, col in enumerate(band32_cols):
            logger.info(f"Writing uint32 band {i + 1}: {col}")
            band_sparse = create_band_array(gdf, col, dtype=np.uint32)
            band_dense = band_sparse.toarray()
            band_dense = np.flip(band_dense, axis=0)
            dst.write(band_dense.astype(np.uint32), i + 1)
            del band_sparse, band_dense
            gc.collect()
        # Then writing in the uint16 bands
        for j, col in enumerate(band16_cols):
            band_index = len(band32_cols) + j + 1
            logger.info(f"Writing uint16 band {band_index}: {col}")
            band_sparse = create_band_array(gdf, col, dtype=np.uint16)
            band_dense = band_sparse.toarray()
            band_dense = np.flip(band_dense, axis=0)
            dst.write(band_dense.astype(np.uint16), band_index)
            del band_sparse, band_dense
            gc.collect()
    logger.info(f"All {total_bands} bands written to {output_path}.")
    return None
