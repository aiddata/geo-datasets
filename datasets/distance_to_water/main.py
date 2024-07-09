import os
import shutil
from pathlib import Path
from typing import List
from zipfile import ZipFile

import distancerasters as dr
import numpy as np
import requests
from affine import Affine
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class DISTANCE_TO_WATER_Configuration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    gshhg_version: str
    ne_hash: str
    pixel_size: float
    download_files: List[str]
    raster_type: List[str]
    overwrite_download: bool
    overwrite_extract: bool
    overwrite_binary_raster: bool
    overwrite_distance_raster: bool


class DISTANCE_TO_WATER(Dataset):

    name = "Distance to Water"

    def __init__(self, config: DISTANCE_TO_WATER_Configuration):

        self.gshhg_version = config.gshhg_version
        self.ne_hash = config.ne_hash

        self.version = f"{config.gshhg_version}_{config.ne_hash[:7]}"

        self.raw_dir = Path(config.raw_dir) / self.version
        self.output_dir = Path(config.output_dir) / self.version

        self.pixel_size = config.pixel_size
        self.download_list = config.download_files
        self.raster_type = config.raster_type

        self.overwrite_download = config.overwrite_download
        self.overwrite_extract = config.overwrite_extract
        self.overwrite_binary_raster = config.overwrite_binary_raster
        self.overwrite_distance_raster = config.overwrite_distance_raster

    def raster_conditional(self, rarray):
        return rarray == 1

    def test_connection(self):
        # test connection
        test_request = requests.get(
            "https://www.soest.hawaii.edu/pwessel/gshhg/", verify=True
        )
        test_request.raise_for_status()

    def manage_download(self, download_dest):
        """
        Download individual file
        """
        logger = self.get_logger()
        if (
            download_dest
            == f"http://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-{self.gshhg_version}.zip"
        ):
            dir_name = self.raw_dir / "gshhg"
            os.makedirs(dir_name, exist_ok=True)
            local_filename = (
                self.raw_dir / "gshhg" / f"gshhg-shp-{self.gshhg_version}.zip"
            )
        elif (
            download_dest
            == f"https://github.com/nvkelso/natural-earth-vector/archive/{self.ne_hash}.zip"
        ):
            dir_name = self.raw_dir / "natural-earth-vector"
            os.makedirs(dir_name, exist_ok=True)
            local_filename = (
                self.raw_dir
                / "natural-earth-vector"
                / f"natural-earth-vector-{self.ne_hash}.zip"
            )

        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {local_filename}")
        else:
            with requests.get(download_dest, stream=True, verify=True) as r:
                r.raise_for_status()
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            logger.info(f"Downloaded: {download_dest}")
        return (download_dest, local_filename)

    def build_extract_list(self):
        """
        Prepare file list to extract
        """
        logger = self.get_logger()

        task_list = []

        gshhg_zip_name = self.raw_dir / "gshhg" / f"gshhg-shp-{self.gshhg_version}.zip"

        for ext in ["shp", "shx", "prj", "dbf"]:

            zip_shore_shp_file = f"GSHHS_shp/f/GSHHS_f_L1.{ext}"
            output_shore_shp_file = self.raw_dir / "gshhg" / f"GSHHS_f_L1.{ext}"
            if os.path.isfile(output_shore_shp_file) and not self.overwrite_extract:
                logger.info(f"File previously extracted: {output_shore_shp_file}")
            else:
                task_list.append(
                    (gshhg_zip_name, zip_shore_shp_file, output_shore_shp_file)
                )

        ne_zip_name = (
            self.raw_dir
            / "natural-earth-vector"
            / f"natural-earth-vector-{self.ne_hash}.zip"
        )

        for ext in ["shp", "shx", "prj", "dbf"]:

            zip_lakes_shp_file = (
                f"natural-earth-vector-{self.ne_hash}/10m_physical/ne_10m_lakes.{ext}"
            )
            output_lakes_shp_file = (
                self.raw_dir / "natural-earth-vector" / f"ne_10m_lakes.{ext}"
            )
            if os.path.isfile(output_lakes_shp_file) and not self.overwrite_extract:
                logger.info(f"File previously extracted: {output_lakes_shp_file}")
            else:
                task_list.append(
                    (ne_zip_name, zip_lakes_shp_file, output_lakes_shp_file)
                )

        for ext in ["shp", "shx", "prj", "dbf"]:

            zip_rivers_shp_file = f"natural-earth-vector-{self.ne_hash}/10m_physical/ne_10m_rivers_lake_centerlines.{ext}"
            output_rivers_shp_file = (
                self.raw_dir
                / "natural-earth-vector"
                / f"ne_10m_rivers_lake_centerlines.{ext}"
            )
            if os.path.isfile(output_rivers_shp_file) and not self.overwrite_extract:
                logger.info(f"File previously extracted: {output_rivers_shp_file}")
            else:
                task_list.append(
                    (ne_zip_name, zip_rivers_shp_file, output_rivers_shp_file)
                )

        return task_list

    def extract_files(self, zip_path, zip_file, dst_path):
        """
        Extract files needed to create rasters
        """
        logger = self.get_logger()
        if os.path.isfile(dst_path) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {dst_path}")
        else:
            with ZipFile(zip_path) as myzip:
                with myzip.open(zip_file) as src:
                    with open(dst_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)

            if not os.path.isfile(dst_path):
                logger.info(f"Error extracting: {dst_path}")
                raise Exception("File extracted but not found at destination")
            else:
                logger.info(f"File extracted: {dst_path}")
        file_path = zip_path / zip_file
        return (file_path, dst_path)

    def create_raster(self, type):
        """
        Create binary and distance raster for borders
        """
        logger = self.get_logger()
        return_list = []

        logger.info("Preparing rasterization")
        pixel_size = self.pixel_size
        xmin = -180
        xmax = 180
        ymin = -90
        ymax = 90
        affine = Affine(pixel_size, 0, xmin, 0, -pixel_size, ymax)
        shape = (int((ymax - ymin) / pixel_size), int((xmax - xmin) / pixel_size))

        shorelines_path = str(self.raw_dir) + "/gshhg/GSHHS_f_L1.shp"
        shorelines, _ = dr.rasterize(shorelines_path, affine=affine, shape=shape)
        shorelines = np.logical_not(shorelines).astype(int)

        lakes_path = str(self.raw_dir) + "/natural-earth-vector/ne_10m_lakes.shp"
        lakes, _ = dr.rasterize(lakes_path, affine=affine, shape=shape)

        rivers_path = (
            str(self.raw_dir)
            + "/natural-earth-vector/ne_10m_rivers_lake_centerlines.shp"
        )
        rivers, _ = dr.rasterize(rivers_path, affine=affine, shape=shape)

        water = shorelines + lakes + rivers

        if type == "binary":
            logger.info("Creating binary raster")
            water_output_raster_path = str(self.output_dir) + "/water_binary.tif"
            if (
                os.path.isfile(water_output_raster_path)
                and not self.overwrite_distance_raster
            ):
                logger.info(f"Raster previously created: {water_output_raster_path}")
            else:
                try:
                    dr.export_raster(water, affine, water_output_raster_path)
                    logger.info(f"Water raster created: {water_output_raster_path}")
                    return ("Success", str(water_output_raster_path))
                except Exception as e:
                    logger.info(
                        f"Error creating distance raster {water_output_raster_path}: {e}"
                    )
                    return (str(e), str(water_output_raster_path))

        elif type == "distance":
            logger.info("Creating distance raster")
            distance_output_raster_path = str(self.output_dir) + "/water_distance.tif"
            if (
                os.path.isfile(distance_output_raster_path)
                and not self.overwrite_distance_raster
            ):
                logger.info(f"Raster previously created: {distance_output_raster_path}")
            else:
                try:
                    dr.DistanceRaster(
                        water,
                        affine=affine,
                        output_path=distance_output_raster_path,
                        conditional=self.raster_conditional,
                    )
                    logger.info(
                        f"Distance raster created: {distance_output_raster_path}"
                    )
                    return_list.append(("Success", str(distance_output_raster_path)))
                except Exception as e:
                    logger.info(
                        f"Error creating distance raster {distance_output_raster_path}: {e}"
                    )
                    return_list.append((str(e), str(distance_output_raster_path)))
        return return_list

    def main(self):
        logger = self.get_logger()

        os.makedirs(self.raw_dir, exist_ok=True)

        logger.info("Running data download")
        download = self.run_tasks(
            self.manage_download, [[f] for f in self.download_list]
        )
        self.log_run(download)

        logger.info("Building extract list...")
        extract_list = self.build_extract_list()

        os.makedirs(self.output_dir, exist_ok=True)
        if len(extract_list) != 0:
            logger.info("Extracting raw files")
            extraction = self.run_tasks(self.extract_files, extract_list)
            self.log_run(extraction)

        logger.info("Creating rasters")
        create_raster = self.run_tasks(
            self.create_raster, [[f] for f in self.raster_type]
        )
        self.log_run(create_raster)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def distance_to_water(config: DISTANCE_TO_WATER_Configuration):
        DISTANCE_TO_WATER(config).run(config.run)


if __name__ == "__main__":
    config = get_config(DISTANCE_TO_WATER_Configuration)
    DISTANCE_TO_WATER(config).run(config.run)
