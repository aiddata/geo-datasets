
import distancerasters as dr
import os
import sys
import requests
from affine import Affine
from pathlib import Path
import shutil
from zipfile import ZipFile
from configparser import ConfigParser

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset

class DISTANCE_TO_BORDERS(Dataset):
    name = "DISTANCE_TO_BORDERS"

    def __init__(self, raw_dir, output_dir, overwrite_download=False, overwrite_extract=False, overwrite_binary_raster=False, overwrite_distance_raster=False):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract
        self.overwrite_binary_raster = overwrite_binary_raster
        self.overwrite_distance_raster = overwrite_distance_raster
    
    def raster_conditional(self, rarray):
        return (rarray == 1)
    
    def test_connection(self):
        # test connection
        test_request = requests.get("https://www.geoboundaries.org/index.html", verify=True)
        test_request.raise_for_status()
    
    def manage_download(self):
        """
        Download individual file
        """

        logger = self.get_logger()

        download_dest = "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM0.zip"
        local_filename = self.raw_dir / "geoBoundariesCGAZ_ADM0.zip"

        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {local_filename}")
        else:
            with requests.get(download_dest, stream=True, verify=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)
            logger.info(f"Downloaded: {download_dest}")

        return (self, download_dest, local_filename)
    
    def build_extract_list(self):
        """
        Prepare file list to extract
        """
        logger = self.get_logger()
        
        zip_name = self.raw_dir / "geoBoundariesCGAZ_ADM0.zip"
        task_list = []
        zip_shp_file = "geoBoundariesCGAZ_ADM0.shp"
        output_shp_file = self.raw_dir / "geoBoundariesCGAZ_ADM0.shp"
        if os.path.isfile(output_shp_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_shp_file}")
        else:
            task_list.append((zip_name, zip_shp_file, output_shp_file))

        zip_shx_file = "geoBoundariesCGAZ_ADM0.shx"
        output_shx_file = self.raw_dir / "geoBoundariesCGAZ_ADM0.shx"
        if os.path.isfile(output_shx_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_shx_file}")
        else:
            task_list.append((zip_name, zip_shx_file, output_shx_file))
        
        zip_prj_file = "geoBoundariesCGAZ_ADM0.prj"
        output_prj_file = self.raw_dir / "geoBoundariesCGAZ_ADM0.prj"
        if os.path.isfile(output_prj_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_prj_file}")
        else:
            task_list.append((zip_name, zip_prj_file, output_prj_file))
        
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

    def create_raster(self):
        """
        Create binary and distance raster for borders
        """
        logger = self.get_logger()
        return_list = []
        
        logger.info("Preparing rasterization")
        pixel_size = 0.01
        xmin = -180
        xmax = 180
        ymin = -90            
        ymax = 90
        affine = Affine(pixel_size, 0, xmin, 0, -pixel_size, ymax)
        shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))
        borders_path = str(self.raw_dir) + "/geoBoundariesCGAZ_ADM0.shp"
        borders, _ = dr.rasterize(borders_path, affine=affine, shape=shape)

        logger.info("Creating binary borders raster")
        borders_output_raster_path = self.output_dir / "binary" / "geoboundaries_borders_binary.tif"
        if os.path.isfile(borders_output_raster_path) and not self.overwrite_binary_raster:
            logger.info(f"Raster previously created: {borders_output_raster_path}")
        else:
            try:
                dr.export_raster(borders, affine, borders_output_raster_path)
                logger.info(f"Binary raster created: {borders_output_raster_path}")
                return_list.append(("Success", str(borders_output_raster_path)))
            except Exception as e:
                logger.info(f"Error creating binary raster {borders_output_raster_path}: {e}")
                return_list.append((str(e), str(borders_output_raster_path)))

        logger.info("Creating distance raster")
        distance_output_raster_path = self.output_dir / "geoboundaries_borders_distance.tif"
        if os.path.isfile(distance_output_raster_path) and not self.overwrite_distance_raster:
            logger.info(f"Raster previously created: {distance_output_raster_path}")
        else:
            try:
                dr.DistanceRaster(borders, affine=affine, output_path=distance_output_raster_path, conditional=self.raster_conditional)
                logger.info(f"Distance raster created: {distance_output_raster_path}")
                return_list.append(("Success", str(distance_output_raster_path)))
            except Exception as e:
                logger.info(f"Error creating distance raster {distance_output_raster_path}: {e}")
                return_list.append((str(e), str(distance_output_raster_path)))
        return return_list
    
    
    def main(self):
        logger = self.get_logger()

        os.makedirs(self.raw_dir, exist_ok=True)

        logger.info("Testing Connection...")
        self.test_connection()

        logger.info("Running data download")
        download = self.manage_download()

        logger.info("Building extract list...")
        extract_list = self.build_extract_list()

        os.makedirs(self.output_dir, exist_ok=True)
        if len(extract_list) != 0:
            logger.info("Extracting raw files")
            extraction = self.run_tasks(self.extract_files, extract_list)
            self.log_run(extraction)

        logger.info("Creating rasters")
        create_raster = self.create_raster()

def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "log_dir": Path(config["main"]["output_dir"]) / "logs",
            "backend": config["run"]["backend"],
            "task_runner": config["run"]["task_runner"],
            "run_parallel": config["run"].getboolean("run_parallel"),
            "max_workers": int(config["run"]["max_workers"]),
            "cores_per_process": int(config["run"]["cores_per_process"]),
            "overwrite_download": config["main"].getboolean("overwrite_download"),
            "overwrite_extract": config["main"].getboolean("overwrite_extract"),
            "overwrite_binary_raster": config["main"].getboolean("overwrite_binary_raster"),
            "overwrite_distance_raster": config["main"].getboolean("overwrite_distance_raster")
        }

if __name__ == "__main__":
    config_dict = get_config_dict()

    class_instance = DISTANCE_TO_BORDERS(config_dict["raw_dir"], config_dict["output_dir"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_binary_raster"], config_dict["overwrite_distance_raster"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])

