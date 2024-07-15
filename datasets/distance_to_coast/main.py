import os
import shutil
from pathlib import Path
from typing import List
from zipfile import ZipFile

import distancerasters as dr
import requests
from affine import Affine
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class DISTANCE_TO_COAST_Configuration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    gshhg_version: str
    pixel_size: float
    download_dest: List[str]
    raster_type: List[str]
    overwrite_download: bool
    overwrite_extract: bool
    overwrite_binary_raster: bool
    overwrite_distance_raster: bool


class DISTANCE_TO_COAST(Dataset):

    name = "Distance to Coast"

    def __init__(self, config: DISTANCE_TO_COAST_Configuration):

        self.gshhg_version = config.gshhg_version
        self.version = config.gshhg_version

        self.raw_dir = Path(config.raw_dir) / self.version
        self.output_dir = Path(config.output_dir) / self.version

        self.pixel_size = config.pixel_size
        self.download_dest = config.download_dest
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
        local_filename = self.raw_dir / f"gshhg-shp-{self.gshhg_version}.zip"

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

        zip_name = self.raw_dir / f"gshhg-shp-{self.gshhg_version}.zip"
        task_list = []

        for ext in ["shp", "shx", "prj", "dbf"]:
            zip_shp_file = f"GSHHS_shp/f/GSHHS_f_L1.{ext}"
            output_shp_file = self.raw_dir / f"GSHHS_f_L1.{ext}"
            if os.path.isfile(output_shp_file) and not self.overwrite_extract:
                logger.info(f"File previously extracted: {output_shp_file}")
            else:
                task_list.append((zip_name, zip_shp_file, output_shp_file))

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
        borders_path = str(self.raw_dir) + "/GSHHS_f_L1.shp"
        borders, _ = dr.rasterize(borders_path, affine=affine, shape=shape)

        if type == "binary":
            logger.info("Creating binary borders raster")
            borders_output_raster_path = (
                self.output_dir / "binary" / "GSHHS_coasts_binary.tif"
            )
            borders_output_raster_path.parent.mkdir(parents=True, exist_ok=True)
            if (
                os.path.isfile(borders_output_raster_path)
                and not self.overwrite_binary_raster
            ):
                logger.info(f"Raster previously created: {borders_output_raster_path}")
            else:
                try:
                    dr.export_raster(borders, affine, borders_output_raster_path)
                    logger.info(f"Binary raster created: {borders_output_raster_path}")
                    return_list.append(("Success", str(borders_output_raster_path)))
                except Exception as e:
                    logger.info(
                        f"Error creating binary raster {borders_output_raster_path}: {e}"
                    )
                    return_list.append((str(e), str(borders_output_raster_path)))

        elif type == "distance":
            logger.info("Creating distance raster")
            distance_output_raster_path = self.output_dir / "distance" / "GSHHS_coasts_distance.tif"
            distance_output_raster_path.parent.mkdir(parents=True, exist_ok=True)
            if (
                os.path.isfile(distance_output_raster_path)
                and not self.overwrite_distance_raster
            ):
                logger.info(f"Raster previously created: {distance_output_raster_path}")
            else:
                try:
                    dr.DistanceRaster(
                        borders,
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
            self.manage_download, [[f] for f in self.download_dest]
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
    def distance_to_coast(config: DISTANCE_TO_COAST_Configuration):
        DISTANCE_TO_COAST(config).run(config.run)


if __name__ == "__main__":
    config = get_config(DISTANCE_TO_COAST_Configuration)
    DISTANCE_TO_COAST(config).run(config.run)
