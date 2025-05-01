from src.helpers import GEOPARQUET_DIR, GRID_SIZE
import geopandas as gpd
import pandas as pd
import numpy as np
import logging
from pyquadkey2.quadkey import QuadKey
from scipy.sparse import coo_matrix
import sys
import os
from tqdm import tqdm

# Logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)



def read_parquet(parquet_file: str) -> gpd.GeoDataFrame:
    """
    Reads the Parquet file and converts the 'tile' column into a geometry column
    and creates a GeoDataFrame
    ----
    Args:
        parquet_file (str) is the path to the parquet file
    Returns:
        gpd.GeoDataFrame or nothing if the parquet file isn't found
    """
    if os.path.exists(parquet_file):
        logger.info(f"Reading Parquet file: {parquet_file}")
        parquet_data = pd.read_parquet(parquet_file)
        parquet_data["geometry"] = gpd.GeoSeries.from_wkt(parquet_data["tile"])
        gdf = gpd.GeoDataFrame(parquet_data)
        # logger.info(gdf.head()) # Checking the geoDataFrame
        return gdf
    else:
        logger.warning(f"Parquet file not found: {parquet_file}")
        return None


def quadkey_to_tile(quadkey: str, grid_size: int = GRID_SIZE) -> tuple[int, int]:
    """
    Converts a quadkey to grid coordinates (x,y) adjusted based on GRID_SIZE
    ----
    Args:
        quadkey (str) is a QuadKey string representing a tile
        grid_size (int) is the size of the raster grid (width x height)
    Returns:
        tuple[int, int] represent the (x,y) coordinates within the grid
    """
    quadkey_obj = QuadKey(quadkey)
    x, y = quadkey_obj.tile
    x_idx = x % grid_size
    y_idx = grid_size - 1 - (y % grid_size)
    return x_idx, y_idx


def create_band_array(
    gdf: gpd.GeoDataFrame,
    band_column: str,
    grid_size: int = GRID_SIZE,
    dtype=np.float32,
) -> coo_matrix:
    """
    Creates a sparse 2D raster band array from GeoDataFrame using
    quadkeys, transformed from quadkey_to_tile()
    ----
    Args:
        gdf (GeoDataFrame): input geospatial data
        band_column (str): column name to populate the band
        grid_size (int): the dimensions of the raster (width x height)
        dtype (np.dtype): data type of values
    Returns:
        coo_matrix: 2D array of shape (GRID_SIZE, GRID_SIZE)
    """
    # lists for coordinates and band array values
    coords_y = []
    coords_x = []
    values = []
    # looping for the length of the geoDataFrame
    for idx in tqdm(range(len(gdf))):
        try:
            row = gdf.iloc[idx]
            quadkey = row["quadkey"]
            x, y = quadkey_to_tile(quadkey)
            value = row.get(band_column, np.nan)
            if not np.isnan(value):
                coords_y.append(y)
                coords_x.append(x)
                values.append(value)
        except Exception as e:
            logger.error(f"Error  processing row {idx} for band '{band_column}': {e} ")
    sparse_array = coo_matrix(
        (values, (coords_y, coords_x)),
        shape=(grid_size, grid_size),
        dtype=dtype
    )
    logger.info(f"Successfully created sparse array for band '{band_column}'")
    return sparse_array
