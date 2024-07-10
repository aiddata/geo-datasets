import tarfile
from pathlib import Path
from typing import List

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from rasterio import features


class UDelClimateConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    methods: List[str]
    build_monthly: bool
    build_yearly: bool
    overwrite_download: bool
    overwrite_processing: bool


class UDelClimate(Dataset):
    name = "UDel Climate"

    def __init__(self, config: UDelClimateConfiguration):

        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.methods = config.methods
        self.build_monthly = config.build_monthly
        self.build_yearly = config.build_yearly
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

    def test_connection(self):
        logger = self.get_logger()
        logger.info(f"Testing download connection...")
        # test connection
        test_request = requests.get(
            "http://climate.geog.udel.edu/~climate/html_pages/Global2017", verify=True
        )
        test_request.raise_for_status()

    def download(self):
        logger = self.get_logger()
        logger.info("Downloading files...")

        tmp_readme_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/README.GlobalTsT2017.html"
        tmp_data_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/air_temp_2017.tar.gz"
        pre_readme_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/README.GlobalTsP2017.html"
        pre_data_url = "http://climate.geog.udel.edu/~climate/html_pages/Global2017/precip_2017.tar.gz"

        download_urls = [tmp_readme_url, tmp_data_url, pre_readme_url, pre_data_url]

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # download data
        for url in download_urls:
            fname = url.split("/")[-1]
            fpath = self.raw_dir / fname
            if not fpath.exists() or self.overwrite_download:
                logger.info(f"\tdownloading {url}...")
                r = requests.get(url, allow_redirects=True)
                with open(fpath, "wb") as dst:
                    dst.write(r.content)

    def extract(self):
        logger = self.get_logger()
        logger.info("Extracting files...")

        # extract
        extract_list = list(self.raw_dir.glob("*.tar.gz"))

        for fpath in extract_list:
            dirname = str(fpath).split(".")[0]
            logger.info(f"\textracting {fpath}...")
            with tarfile.open(fpath) as tar:
                tar.extractall(path=self.raw_dir / dirname)

    def gdf_to_raster(self, gdf, out_path, meta, value_col):

        shapes = list(
            (geom, value) for geom, value in zip(gdf.geometry, gdf[value_col])
        )
        out = features.rasterize(
            list(shapes),
            out_shape=(meta["height"], meta["width"]),
            fill=meta["nodata"],
            transform=meta["transform"],
            dtype=meta["dtype"],
        )

        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(np.array([out]))

    def convert_file(self, dataset, fpath):
        logger = self.get_logger()
        logger.info(f"Converting {fpath}...")

        months = [f"{i:02d}" for i in range(1, 13)]

        meta = {
            "driver": "COG",
            "compress": "LZW",
            "dtype": "float32",
            "height": 360,
            "width": 720,
            "count": 1,
            "crs": "EPSG:4326",
            "transform": rasterio.Affine(0.5, 0.0, -180.0, 0.0, -0.5, 90.0),
            "nodata": -9999.0,
        }

        year = fpath.name.split(".")[1]

        # load csv to gdf
        data = pd.read_csv(fpath, sep="\s+", header=None)
        data.columns = ["lon", "lat"] + months + ["extra"]

        gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.lon, data.lat))
        gdf = gdf.set_crs(epsg=4326)

        # monthly
        if self.build_monthly:
            for m in months:

                out_path = (
                    self.output_dir
                    / dataset
                    / "monthly"
                    / year
                    / f"{dataset[-5]}_{year}_{m}.tif"
                )

                if out_path.exists() and not self.overwrite_processing:
                    logger.info(f"\tmonthly {year}_{m} exists, skipping...")

                else:
                    logger.info(f"\tbuilding monthly {year}_{m}...")
                    out_path.parent.mkdir(parents=True, exist_ok=True)

                    self.gdf_to_raster(gdf, out_path, meta, value_col=m)

        # yearly
        if self.build_yearly:
            for j in self.methods:

                out_path = (
                    self.output_dir
                    / dataset
                    / "yearly"
                    / j
                    / f"{dataset[-5]}_{year}_{j}.tif"
                )

                if out_path.exists() and not self.overwrite_processing:
                    logger.info(f"\tyearly {year}_{j} exists, skipping...")

                else:
                    logger.info(f"\tbuilding yearly {year}_{j}...")
                    out_path.parent.mkdir(parents=True, exist_ok=True)

                    gdf[f"year_{j}"] = gdf[months].apply(j, axis=1)
                    self.gdf_to_raster(gdf, out_path, meta, value_col=f"year_{j}")

    def prepare_conversion_tasks(self):
        logger = self.get_logger()
        logger.info(f"Preparing conversion tasks...")

        extract_list = list(self.raw_dir.glob("*.tar.gz"))

        # process
        data_dirname_list = [str(i).split("/")[-1].split(".")[0] for i in extract_list]

        flist = [(i, list((self.raw_dir / i).glob("*"))) for i in data_dirname_list]

        if len(flist) == 0 or len(flist[0][1]) == 0 or len(flist[1][1]) == 0:
            raise Exception(f"no files found ({self.raw_dir})")

        task_list = []

        for dataset, data_files in flist:
            for fpath in data_files:

                task_list.append([dataset, fpath])

        return task_list

    def main(self):
        logger = self.get_logger()

        logger.info("Running intial tasks...")

        self.test_connection()
        self.download()
        self.extract()

        logger.info("Running conversion tasks...")

        conv_list = self.prepare_conversion_tasks()
        conv = self.run_tasks(self.convert_file, conv_list)
        self.log_run(conv)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def udel_climate(config: UDelClimateConfiguration):
        UDelClimate(config).run(config.run)


if __name__ == "__main__":
    config = get_config(UDelClimateConfiguration)
    UDelClimate(config).run(config.run)
