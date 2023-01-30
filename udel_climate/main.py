
import sys
import os
from pathlib import Path
import tarfile
import requests
from datetime import datetime
from configparser import ConfigParser

import rasterio
from rasterio import features
import numpy as np
import pandas as pd
import geopandas as gpd


sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class UDelClimate(Dataset):
    name = "UDel Climate"

    def __init__(self, raw_dir, output_dir, methods, build_monthly=True, build_yearly=True, overwrite_download=False, overwrite_processing=False):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.methods = methods
        self.build_monthly = build_monthly
        self.build_yearly = build_yearly
        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing


    def test_connection(self):
        logger = self.get_logger()
        logger.info(f'Testing download connection...')
        # test connection
        test_request = requests.get("http://climate.geog.udel.edu/~climate/html_pages/Global2017", verify=True)
        test_request.raise_for_status()


    def download(self):
        logger = self.get_logger()
        logger.info('Downloading files...')

        tmp_readme_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/README.GlobalTsT2017.html"
        tmp_data_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/air_temp_2017.tar.gz"
        pre_readme_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/README.GlobalTsP2017.html"
        pre_data_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/precip_2017.tar.gz"

        download_urls = [tmp_readme_url, tmp_data_url, pre_readme_url, pre_data_url]

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # download data
        for url in download_urls:
            fname = url.split('/')[-1]
            fpath = self.raw_dir / fname
            if not fpath.exists() or self.overwrite_download:
                logger.info(f'\tdownloading {url}...')
                r = requests.get(url, allow_redirects=True)
                with open(fpath, 'wb') as dst:
                    dst.write(r.content)


    def extract(self):
        logger = self.get_logger()
        logger.info('Extracting files...')

        # extract
        extract_list = list(self.raw_dir.glob('*.tar.gz'))

        for fpath in extract_list:
            dirname = str(fpath).split('.')[0]
            logger.info(f'\textracting {fpath}...')
            with tarfile.open(fpath) as tar:
                tar.extractall(path=self.raw_dir/dirname)


    def gdf_to_raster(self, gdf, out_path, meta, value_col):

        shapes = list((geom, value) for geom, value in zip(gdf.geometry, gdf[value_col]))
        out = features.rasterize(list(shapes), out_shape=(meta['height'], meta['width']), fill=meta['nodata'], transform=meta['transform'], dtype=meta['dtype'])

        with rasterio.open(out_path, 'w', **meta) as dst:
            dst.write(np.array([out]))


    def convert_file(self, dataset, fpath):
        logger = self.get_logger()
        logger.info(f'Converting {fpath}...')

        months = [f'{i:02d}' for i in range(1, 13)]

        meta = {
            'driver': 'COG',
            'compress': 'LZW',
            'dtype': 'float32',
            'height': 360,
            'width': 720,
            'count': 1,
            'crs': 'EPSG:4326',
            'transform': rasterio.Affine(0.5, 0.0, -180.0, 0.0, -0.5, 90.0),
            'nodata': -9999.0,
        }

        year = fpath.name.split('.')[1]

        # load csv to gdf
        data = pd.read_csv(fpath, sep='\s+', header=None)
        data.columns = ['lon', 'lat'] + months + ["extra"]

        gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.lon, data.lat))
        gdf = gdf.set_crs(epsg=4326)


        # monthly
        if self.build_monthly:
            for m in months:

                out_path = self.output_dir / dataset / 'monthly' / year / f"{dataset[-5]}_{year}_{m}.tif"

                if out_path.exists() and not self.overwrite_processing:
                    logger.info(f'\tmonthly {year}_{m} exists, skipping...')

                else:
                    logger.info(f'\tbuilding monthly {year}_{m}...')
                    out_path.parent.mkdir(parents=True, exist_ok=True)

                    self.gdf_to_raster(gdf, out_path, meta, value_col=m)


        # yearly
        if self.build_yearly:
            for j in self.methods:

                out_path = self.output_dir / dataset / 'yearly' / j / f"{dataset[-5]}_{year}.tif"

                if out_path.exists() and not self.overwrite_processing:
                    logger.info(f'\tyearly {year}_{j} exists, skipping...')

                else:
                    logger.info(f'\tbuilding yearly {year}_{j}...')
                    out_path.parent.mkdir(parents=True, exist_ok=True)

                    gdf[f"year_{j}"] = gdf[months].apply(j, axis=1)
                    self.gdf_to_raster(gdf, out_path, meta, value_col=f"year_{j}")


    def prepare_conversion_tasks(self):
        logger = self.get_logger()
        logger.info(f'Preparing conversion tasks...')

        extract_list = list(self.raw_dir.glob('*.tar.gz'))

        # process
        data_dirname_list = [str(i).split('/')[-1].split('.')[0] for i in extract_list]

        flist = [(i, list((self.raw_dir / i).glob('*'))) for i in data_dirname_list]

        if len(flist) == 0 or len(flist[0][1]) == 0 or len(flist[1][1]) == 0:
            raise Exception(f'no files found ({self.raw_dir})')


        task_list = []

        for dataset, data_files in flist:
            for fpath in data_files:

                task_list.append([dataset, fpath])

        return task_list


    def main(self):
        logger = self.get_logger()

        logger.info('Running intial tasks...')

        self.test_connection()
        self.download()
        self.extract()

        logger.info('Running conversion tasks...')

        conv_list = self.prepare_conversion_tasks()
        conv = self.run_tasks(self.convert_file, conv_list)
        self.log_run(conv)



def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "methods": [m for m in config["main"]["methods"].split(", ")],
        "build_monthly": config["main"].getboolean("build_monthly"),
        "build_yearly": config["main"].getboolean("build_yearly"),
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs"
    }


if __name__ == "__main__":

    config_dict = get_config_dict()

    log_dir = config_dict["log_dir"]
    timestamp = datetime.today()
    time_format_str: str="%Y_%m_%d_%H_%M"
    time_str = timestamp.strftime(time_format_str)
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)


    class_instance = UDelClimate(config_dict["raw_dir"], config_dict["output_dir"], config_dict["methods"], config_dict["build_monthly"], config_dict["build_yearly"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
