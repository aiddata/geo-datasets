import geopandas as gpd
import os
import logging
import sys

# instantiating the logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def make_geopackage(gdf: gpd.GeoDataFrame, output_path: str):
    """
    Saves a GeoDataFrame as a GeoPackage (.gpkg)
    Note: this is not a crucial step to obtaining raster output
    and is just an option for additional visualization functionality
    """
    try:
        if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            gdf.to_file(output_path, driver="GPKG")
            logger.info(f"Saved data as GeoPackage: {output_path}")
        else:
            logger.warning("Invalid GeoDataFrame provided to make_geopackage.")
    except Exception as e:
        logger.error(f"Failed to save GeoPackage: {e}")
