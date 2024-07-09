import zipfile
from pathlib import Path

import fiona
import geopandas as gpd
import numpy as np
import rasterio
import requests
from affine import Affine
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from rasterio import features


class WDPAConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    download_url: str
    max_retries: int
    overwrite_download: bool
    overwrite_output: bool


class WDPA(Dataset):

    name = "WDPA"

    def __init__(self, config: WDPAConfiguration):

        self.download_url = config.download_url
        self.download_path = Path(config.download_url)
        self.version = str(self.download_path.stem.split("_")[1])

        self.raw_dir = Path(config.raw_dir) / self.version
        self.zip_path = self.raw_dir / self.download_path.name
        self.gdb_path = self.raw_dir / f"{self.download_path.stem}.gdb"
        self.output_dir = Path(config.output_dir) / self.version / "iucn_cat"
        self.output_path = self.output_dir / "wdpa_iucn_cat.tif"

        self.max_retries = config.max_retries

        self.overwrite_download = config.overwrite_download
        self.overwrite_output = config.overwrite_output

        self.pixel_size = 0.01

        self.field_name = "IUCN_CAT"
        self.field_values = [
            "Ia",
            "Ib",
            "II",
            "III",
            "IV",
            "V",
            "VI",
            "Not Applicable",
            "Not Assigned",
            "Not Reported",
        ]

        try:
            self.pixel_size = float(self.pixel_size)
        except:
            raise Exception("Invalid pixel size (could not be converted to float)")

        self.out_shape = (int(180 / self.pixel_size), int(360 / self.pixel_size))

        self.affine = Affine(self.pixel_size, 0, -180, 0, -self.pixel_size, 90)

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
                    return (self.download_url, self.zip_path)
                else:
                    logger.info(f"Attempt {str(attempts)} : {str(self.download_url)}")

    def extract_data(self):
        """Extract data from downloaded zip file"""

        logger = self.get_logger()

        if self.gdb_path.exists() and not self.overwrite_download:
            logger.info(f"Extract Exists: {self.zip_path}")
        elif not self.zip_path.exists():
            logger.info(f"Error: Data download not found: {self.zip_path}")
            raise Exception(f"Data file not found: {self.zip_path}")
        else:
            logger.info(f"Extracting: {self.zip_path}")
            # extract zipfile to raw_dir
            with zipfile.ZipFile(self.zip_path, "r") as zip_ref:
                zip_ref.extractall(self.raw_dir)

        layers = fiona.listlayers(self.gdb_path)
        poly_layers = [i for i in layers if "poly" in i]

        if len(poly_layers) > 1:
            raise Exception("multiple potential polygon layers found")
        elif len(poly_layers) == 0:
            raise Exception("no potential polygon layer found")

        self.poly_layer = poly_layers[0]

    def process_data(self):
        """Rasterize features from gdb file"""
        logger = self.get_logger()

        logger.info("Loading features")
        # load features from gdb
        input_features = gpd.read_file(self.gdb_path, layer=self.poly_layer)

        # init output raster
        output_raster = np.zeros(shape=(self.out_shape[0], self.out_shape[1]))

        logger.info("Building categories")
        # iterate over the 10 categories and rasterize each, then
        # add rasterized layer to output raster and assign pixels
        # associated with multiple feature across categories to 11
        for index, cat in enumerate(self.field_values):

            features_filtered = input_features.loc[
                input_features[self.field_name] == cat, "geometry"
            ]

            logger.info(
                "selected {0} features for field: {1}".format(
                    len(features_filtered), cat
                )
            )

            if len(features_filtered) == 0:
                logger.info("\tno feature selected for year {0}".format(cat))
                pass

            cat_raster = features.rasterize(
                features_filtered,
                out_shape=self.out_shape,
                transform=self.affine,
                fill=0,
                default_value=index + 1,
                all_touched=True,
                dtype=None,
            )

            output_raster = output_raster + cat_raster

            output_raster = np.where(output_raster > (index + 1), 11, output_raster)

        # export the finalized raster
        logger.info("Exporting raster")
        meta = {
            "count": 1,
            "crs": {"init": "epsg:4326"},
            "dtype": "uint8",
            "transform": self.affine,
            "driver": "GTiff",
            "height": output_raster.shape[0],
            "width": output_raster.shape[1],
            "nodata": 0,
            "compress": "lzw",
        }

        raster_out = np.array([output_raster])

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(self.output_path, "w", **meta) as dst:
            dst.write(raster_out)

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
    def wdpa(config: WDPAConfiguration):
        WDPA(config).run(config.run)


if __name__ == "__main__":
    config = get_config(WDPAConfiguration)
    WDPA(config).run(config.run)
