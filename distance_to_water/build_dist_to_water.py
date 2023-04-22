import distancerasters as dr
import os
import sys
import requests
from affine import Affine
import numpy as np
from pathlib import Path
import shutil
from zipfile import ZipFile
from configparser import ConfigParser

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset

class DISTANCE_TO_WATER(Dataset):
    name = "DISTANCE_TO_WATER"

    def __init__(self, raw_dir, output_dir, pixel_size, raster_type, overwrite_download=False, overwrite_extract=False, overwrite_binary_raster=False, overwrite_distance_raster=False):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.pixel_size = pixel_size
        self.raster_type = raster_type
        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract
        self.overwrite_binary_raster = overwrite_binary_raster
        self.overwrite_distance_raster = overwrite_distance_raster
    
    def raster_conditional(self, rarray):
        return (rarray == 1)
    
    def test_connection(self):
        # test connection
        test_request = requests.get("https://www.soest.hawaii.edu/pwessel/gshhg/", verify=True)
        test_request.raise_for_status()
    
    def manage_download(self, download_dest):
        """
        Download individual file
        """
        logger = self.get_logger()
        if download_dest == "http://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip":
            local_filename = self.raw_dir / "gshhg" / "gshhg-shp-2.3.7.zip"
        elif download_dest == "https://github.com/nvkelso/natural-earth-vector/archive/d4533efe3715c55b51f49bc2bde9694bff2bf7b1.zip":
            local_filename = self.raw_dir / "natural-earth-vector" / "natural_earth_vector-d4533efe3715c55b51f49bc2bde9694bff2bf7b1.zip"

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
        
        gshhg_zip_name = self.raw_dir / "gshhg" / "gshhg-shp-2.3.7.zip" 
        task_list = []
        zip_shore_shp_file = "GSHHS_shp/f/GSHHG_f_L1.shp"
        output_shore_shp_file = self.raw_dir / "gshhg" / "GSHHS_f_L1.shp"
        if os.path.isfile(output_shore_shp_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_shore_shp_file}")
        else:
            task_list.append((gshhg_zip_name, zip_shore_shp_file, output_shore_shp_file))

        zip_shore_shx_file = "GSHHS_shp/f/GSHHS_f_L1.shx"
        output_shore_shx_file = self.raw_dir / "gshhg" / "GSHHS_f_L1.shx"
        if os.path.isfile(output_shore_shx_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_shore_shx_file}")
        else:
            task_list.append((gshhg_zip_name, zip_shore_shx_file, output_shore_shx_file))
        
        zip_shore_prj_file = "GSHHS_shp/f/GSHHS_f_L1.prj"
        output_shore_prj_file = self.raw_dir / "gshhg" / "GSHHS_f_L1.prj"
        if os.path.isfile(output_shore_prj_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_shore_prj_file}")
        else:
            task_list.append((gshhg_zip_name, zip_shore_prj_file, output_shore_prj_file))
        
        zip_shore_dbf_file = "GSHHS_shp/f/GSHHS_f_L1.dbf"
        output_shore_dbf_file = self.raw_dir / "gshhg" / "GSHHS_f_L1.dbf"
        if os.path.isfile(output_shore_dbf_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_shore_dbf_file}")
        else:
            task_list.append((gshhg_zip_name, zip_shore_dbf_file, output_shore_dbf_file))
        
        ne_zip_name = self.raw_dir / "natural-earth-vector" / "natural_earth_vector-d4533efe3715c55b51f49bc2bde9694bff2bf7b1.zip"

        zip_lakes_shp_file = "10m_physical/ne_10m_lakes.shp"
        output_lakes_shp_file = self.raw_dir / "natural-earth-vector" / "ne_10m_lakes.shp"
        if os.path.isfile(output_lakes_shp_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_lakes_shp_file}")
        else:
            task_list.append(ne_zip_name, zip_lakes_shp_file, output_lakes_shp_file)

        zip_lakes_shx_file = "10m_physical/ne_10m_lakes.shx"
        output_lakes_shx_file = self.raw_dir / "natural-earth-vector" / "ne_10m_lakes.shx"
        if os.path.isfile(output_lakes_shx_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_lakes_shx_file}")
        else:
            task_list.append(ne_zip_name, zip_lakes_shx_file, output_lakes_shx_file)
        
        zip_lakes_prj_file = "10m_physical/ne_10m_lakes.prj"
        output_lakes_prj_file = self.raw_dir / "natural-earth-vector" / "ne_10m_lakes.prj"
        if os.path.isfile(output_lakes_prj_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_lakes_prj_file}")
        else:
            task_list.append(ne_zip_name, zip_lakes_prj_file, output_lakes_prj_file)
        
        zip_lakes_dbf_file = "10m_physical/ne_10m_lakes.dbf"
        output_lakes_dbf_file = self.raw_dir / "natural-earth-vector" / "ne_10m_lakes.dbf"
        if os.path.isfile(output_lakes_dbf_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_lakes_dbf_file}")
        else:
            task_list.append(ne_zip_name, zip_lakes_dbf_file, output_lakes_dbf_file)
        
        zip_rivers_shp_file = "10m_physical/ne_10m_rivers_lake_centerlines.shp"
        output_rivers_shp_file = self.raw_dir / "natural-earth-vector" / "ne_10m_rivers_lake_centerlines.shp"
        if os.path.isfile(output_rivers_shp_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_rivers_shp_file}")
        else:
            task_list.append(ne_zip_name, zip_rivers_shp_file, output_rivers_shp_file)

        zip_rivers_shx_file = "10m_physical/ne_10m_rivers_lake_centerlines.shx"
        output_rivers_shx_file = self.raw_dir / "natural-earth-vector" / "ne_10m_rivers_lake_centerlines.shx"
        if os.path.isfile(output_rivers_shx_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_rivers_shx_file}")
        else:
            task_list.append(ne_zip_name, zip_rivers_shx_file, output_rivers_shx_file)
        
        zip_rivers_prj_file = "10m_physical/ne_10m_rivers_lake_centerlines.prj"
        output_rivers_prj_file = self.raw_dir / "natural-earth-vector" / "ne_10m_rivers_lake_centerlines.prj"
        if os.path.isfile(output_rivers_prj_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_rivers_prj_file}")
        else:
            task_list.append(ne_zip_name, zip_rivers_prj_file, output_rivers_prj_file)
        
        zip_rivers_dbf_file = "10m_physical/ne_10m_rivers_lake_centerlines.dbf"
        output_rivers_dbf_file = self.raw_dir / "natural-earth-vector" / "ne_10m_rivers_lake_centerlines.dbf"
        if os.path.isfile(output_rivers_dbf_file) and not self.overwrite_extract:
            logger.info(f"File previously extracted: {output_rivers_dbf_file}")
        else:
            task_list.append(ne_zip_name, zip_rivers_dbf_file, output_rivers_dbf_file)
        
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
        shape = (int((ymax-ymin)/pixel_size), int((xmax-xmin)/pixel_size))

        shorelines_path = str(self.raw_dir) + "/gshhg/GSHHS_f_L1.shp"
        shorelines, _ = dr.rasterize(shorelines_path, affine=affine, shape=shape)
        shorelines = np.logical_not(shorelines).astype(int)

        lakes_path = str(self.raw_dir) + "/natural-earth-vector/ne_10m_lakes.shp"
        lakes, _ = dr.rasterize(lakes_path, affine=affine, shape=shape)

        rivers_path = str(self.raw_dir) + "/natural-earth-vector/ne_10m_rivers_lake_centerline.shp"
        rivers, _ = dr.rasterize(rivers_path, affine=affine, shape=shape)

        water = shorelines + lakes + rivers
        
        if type == "binary":
            logger.info("Creating binary raster")
            water_output_raster_path = str(self.output_dir) + "/water_binary.tif"
            if os.path.isfile(water_output_raster_path) and not self.overwrite_distance_raster:
                logger.info(f"Raster previously created: {water_output_raster_path}")
            else:
                try:
                    dr.export_raster(water, affine, water_output_raster_path)
                    logger.info(f"Water raster created: {water_output_raster_path}")
                    return (("Success", str(distance_output_raster_path)))
                except Exception as e:
                    logger.info(f"Error creating distance raster {water_output_raster_path}: {e}")
                    return ((str(e), str(water_output_raster_path)))

        elif type == "distance":
            logger.info("Creating distance raster")
            distance_output_raster_path = str(self.output_dir) + "/water_distance.tif"
            if os.path.isfile(distance_output_raster_path) and not self.overwrite_distance_raster:
                logger.info(f"Raster previously created: {distance_output_raster_path}")
            else:
                try:
                    dr.DistanceRaster(water, affine=affine, output=distance_output_raster_path, conditional=self.raster_conditional)
                    logger.info(f"Distance raster created: {distance_output_raster_path}")
                    return_list.append(("Success", str(distance_output_raster_path)))
                except Exception as e:
                    logger.info(f"Error creating distance raster {distance_output_raster_path}: {e}")
                    return_list.append((str(e), str(distance_output_raster_path)))
        return return_list
    
    def main(self):
        logger = self.get_logger()

        os.makedirs(self.raw_dir, exist_ok=True)

        download_list = ["http://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip", "https://github.com/nvkelso/natural-earth-vector/archive/d4533efe3715c55b51f49bc2bde9694bff2bf7b1.zip"]

        logger.info("Running data download")
        download = self.run_tasks(self.manage_download, download_list)
        self.log_run(download)

        logger.info("Building extract list...")
        extract_list = self.build_extract_list()

        os.makedirs(self.output_dir, exist_ok=True)
        if len(extract_list) != 0:
            logger.info("Extracting raw files")
            extraction = self.run_tasks(self.extract_files, extract_list)
            self.log_run(extraction)

        logger.info("Creating rasters")
        create_raster = self.run_tasks(self.create_raster, [[f] for f in self.raster_type])
        self.log_run(create_raster)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "log_dir": Path(config["main"]["output_dir"]) / "logs",
            "pixel_size": config["main"].getfloat("pixel_size"),
            "raster_type": [str(y) for y in config["main"]["raster_type"].split(", ")],
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

    class_instance = DISTANCE_TO_WATER(config_dict["raw_dir"], config_dict["output_dir"], config_dict["pixel_size"], config_dict["raster_type"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_binary_raster"], config_dict["overwrite_distance_raster"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])