"""
for use with NDVI product from LTDR raw dataset

- Prepares list of all files
- Builds list of day files to process
- Processes day files
- Builds list of day files to aggregate to months
- Run month aggregation
- Builds list of month files to aggregate to years
- Run year aggregation

example LTDR product file names (ndvi product code is AVH13C1)

AVH13C1.A1981181.N07.004.2013227210959.hdf

split file name by "."
eg:

full file name - "AVH13C1.A1981181.N07.004.2013227210959.hdf"

0     product code        AVH13C1
1     date of image       A1981181
2     sensor code         N07
3     misc                004
4     processed date      2013227210959
5     extension           hdf

"""

import os
import re
import csv
import ssl
import sys
import json
import hashlib
from io import StringIO
from pathlib import Path
from itertools import chain
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict
from configparser import ConfigParser
from typing import Any, Generator, List, Tuple, Type, Union

import rasterio
import requests
import numpy as np
import pandas as pd
from osgeo import gdal, osr


sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class LTDR_NDVI(Dataset):

    name = "Long-term Data Record NDVI"

    def __init__(self,
                 token:str,
                 years: List[Union[int, str]],
                 raw_dir: Union[str, os.PathLike],
                 output_dir: Union[str, os.PathLike],
                 overwrite_download: bool,
                 validate_download: bool,
                 overwrite_processing: bool):

        self.build_list = [
            "daily",
            "monthly",
            "yearly"
        ]

        self.auth_headers = { "Authorization": f"Bearer {token}" }

        self.years = [int(y) for y in years]

        # TODO: warn if raw_dir already points to a directory named "465", it's probably one too deep
        self.raw_dir = Path(raw_dir) / "465"
        self.output_dir = Path(output_dir)

        self.overwrite_download = overwrite_download
        self.validate_download = validate_download
        self.overwrite_processing = overwrite_processing

        self.dataset_url = "https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details/allData/465/"

        self.sensors = [
            "N07_AVH13C1",
            "N09_AVH13C1",
            "N11_AVH13C1",
            "N14_AVH13C1",
            "N16_AVH13C1",
            "N18_AVH13C1",
            "N19_AVH13C1",
        ]


    def build_sensor_download_list(self, sensor: str):
        logger = self.get_logger()

        # generates dictionaries that represent each entry in a directory
        def dir_contents(dir_url: str) -> List[dict]:
            logger.debug(f"Fetching {dir_url}")
            description: dict = json.loads(requests.get(dir_url).content)
            return description["content"]

        # validates md5 hash of a file
        def validate(filepath: Union[str, os.PathLike], md5: str) -> bool:
            with open(filepath, "rb") as chk:
                data = chk.read()
                return md5 == hashlib.md5(data).hexdigest()

        # this is what we'll return
        # list of tuples, each including:
        #   1. a boolean "does the file need to be downloaded?"
        #   2. another tuple: (url_of_download, dst_path_of_download)
        download_list: List[Tuple[bool, Tuple[str, Type[Path]]]] = []

        sensor_dir: str = urljoin(self.dataset_url, sensor)
        # for each year the sensor collected data
        for year_details in dir_contents(sensor_dir):
            # is this a year we'd like data from?
            if int(year_details["name"]) in self.years:
                year_dir: str = "/".join([sensor_dir, year_details["name"]])
                # for each day the sensor collected data in this year
                for day_details in dir_contents(year_dir):
                    day_dir: str = "/".join([year_dir, day_details["name"]])
                    # for each file the sensor created for this day
                    for file_detail in dir_contents(day_dir):
                        day_download_url: str = file_detail["downloadsLink"]
                        dst = self.raw_dir / sensor / year_details["name"] / day_details["name"] / file_detail["name"]
                        # if file is already downloaded, and we aren't in overwrite mode
                        if dst.exists() and not self.overwrite_download:
                            if self.validate_download:
                                if validate(dst, file_detail["md5sum"]):
                                    print(f"INFO: File validated: {dst.as_posix()}")
                                    download_list.append((False, (day_download_url, dst)))
                                else:
                                    print(f"INFO: File validation failed, queuing for download: {dst.as_posix()}")
                                    download_list.append((True, (day_download_url, dst)))
                            else:
                                print(f"INFO: File exists, skipping: {dst.as_posix()}")
                                download_list.append((False, (day_download_url, dst)))
                        else:
                            print(f"INFO: Queuing for download: {day_download_url}")
                            download_list.append((True, (day_download_url, dst)))
        return download_list


    def download(self, src_url: str, final_dst_path: Union[str, os.PathLike]) -> None:
        logger = self.get_logger()
        logger.info(f"Downloading {str(final_dst_path)}...")
        with requests.get(src_url, headers=self.auth_headers, stream=True) as src:
            src.raise_for_status()
            with self.tmp_to_dst_file(final_dst_path) as dst_path:
                with open(dst_path, "wb") as dst:
                    for chunk in src.iter_content(chunk_size=8192):
                        dst.write(chunk)


    def build_process_list(self, downloaded_files):

        # filter options to accept/deny based on sensor, year
        # all values must be strings
        # do not enable/use both accept/deny for a given field

        ops = {
            "use_sensor_accept": False,
            "sensor_accept": [],
            "use_sensor_deny": False,
            "sensor_deny": [],
            "use_year_accept": True,
            "year_accept": ["2019", "2020"],
            "use_year_deny": False,
            "year_deny": ["2019"]
        }

        df_dict_list = []

        for input_path in downloaded_files:
            items = input_path.stem.split(".")
            year = items[1][1:5]
            day = items[1][5:8]
            sensor = items[2]
            month = "{0:02d}".format(datetime.strptime(f"{year}+{day}", "%Y+%j").month)
            output_path = self.output_dir / "daily" / f"avhrr_ndvi_v5_{sensor}_{year}_{day}.tif"
            df_dict_list.append({
                "input_path": input_path,
                "sensor": sensor,
                "year": year,
                "month": month,
                "day": day,
                "year_month": year+"_"+month,
                "year_day": year+"_"+day,
                "output_path": output_path.as_posix()
            })

        df = pd.DataFrame(df_dict_list).sort_values(by=["input_path"])

        # df = df.drop_duplicates(subset="year_day", take_last=True)
        sensors = sorted(list(set(df["sensor"])))
        years = sorted(list(set(df["year"])))
        filter_sensors = None
        if ops['use_sensor_accept']:
            filter_sensors = [i for i in sensors if i in ops['sensor_accept']]
        elif ops['use_sensor_deny']:
            filter_sensors = [i for i in sensors if i not in ops['sensor_deny']]
        if filter_sensors:
            df = df.loc[df["sensor"].isin(filter_sensors)]
        filter_years = None
        if ops['use_year_accept']:
            filter_years = [i for i in years if i in ops['year_accept']]
        elif ops['use_year_deny']:
            filter_years = [i for i in years if i not in ops['year_deny']]
        if filter_years:
            df = df.loc[df["year"].isin(filter_years)]
        return df


    @staticmethod
    def create_mask(qa_array, mask_vals):
        qa_mask_vals = [abs(x - 15) for x in mask_vals]
        mask_bin_array = [0] * 16
        for x in qa_mask_vals:
            mask_bin_array[x] = 1
        mask_bin = int("".join(map(str, mask_bin_array)), 2)

        flag = lambda i: (i & 65535 & mask_bin) != 0

        qa_mask = pd.DataFrame(qa_array).applymap(flag).to_numpy()
        return qa_mask


    def process_daily_data(self, src, dst):
        """
        Process input raster and create output in output directory

        Unpack NDVI subdataset from a HDF container
        Reproject to EPSG:4326
        Set values <0 (other than nodata) to 0
        Write to COG

        Parts of code pulled from:

        https://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
        https://gis.stackexchange.com/questions/42584/how-to-call-gdal-translate-from-python-code
        https://stackoverflow.com/questions/10454316/how-to-project-and-resample-a-grid-to-match-another-grid-with-gdal-python/10538634#10538634
        https://jgomezdans.github.io/gdal_notes/reprojection.html

        Notes:

        Rebuilding geotransform is not really necessary in this case but might
        be useful for future data prep scripts that can use this as startng point.

        """

        logger = self.get_logger()

        year = src.name.split(".")[1][1:5]
        day = src.name.split(".")[1][5:8]
        sensor = src.name.split(".")[2]

        logger.info(f"Processing Day {sensor} {year} {day}")

        input_path = src.as_posix()
        output_path = dst

        # open the dataset and subdataset
        hdf_ds = gdal.Open(input_path, gdal.GA_ReadOnly)

        layers = hdf_ds.GetSubDatasets()

        # ndvi
        ndvi_ds = gdal.Open(layers[0][0], gdal.GA_ReadOnly)
        # qa
        qa_ds = gdal.Open(layers[1][0], gdal.GA_ReadOnly)

        # clean data
        ndvi_array = ndvi_ds.ReadAsArray().astype(np.int16)

        qa_array = qa_ds.ReadAsArray().astype(np.int16)

        # list of qa fields and bit numbers
        # https://ltdr.modaps.eosdis.nasa.gov/ltdr/docs/AVHRR_LTDR_V5_Document.pdf
        # MSB first (invert for Python list lookip)

        qa_bits = {
            15: "Polar flag: latitude > 60deg (land) or > 50deg (ocean)",
            14: "BRDF-correction issues",
            13: "RHO3 value is invalid",
            12: "Channel 5 value is invalid",
            11: "Channel 4 value is invalid",
            10: "Channel 3 value is invalid",
            9: "Channel 2 (NIR) value is invalid",
            8: "Channel 1 (visible) value is invalid",
            7: "Channel 1-5 are invalid",
            6: "Pixel is at night (high solar zenith angle)",
            5: "Pixel is over dense dark vegetation",
            4: "Pixel is over sun glint",
            3: "Pixel is over water",
            2: "Pixel contains cloud shadow",
            1: "Pixel is cloudy",
            0: "Unused"
        }

        # qa_mask_vals = [15, 9, 8, 6, 4, 3, 2, 1]
        qa_mask_vals = [15, 9, 8, 1]

        qa_mask = self.create_mask(qa_array, qa_mask_vals)

        ndvi_array[qa_mask] = -9999

        ndvi_array[np.where((ndvi_array < 0) & (ndvi_array > -9999))] = 0
        ndvi_array[np.where(ndvi_array > 10000)] = 10000

        # -----------------

        # prep projections and transformations
        src_proj = osr.SpatialReference()
        src_proj.ImportFromWkt(ndvi_ds.GetProjection())

        dst_proj = osr.SpatialReference()
        dst_proj.ImportFromEPSG(4326)

        tx = osr.CoordinateTransformation(src_proj, dst_proj)

        src_gt = ndvi_ds.GetGeoTransform()
        pixel_xsize = src_gt[1]
        pixel_ysize = abs(src_gt[5])

        # extents
        (ulx, uly, ulz) = tx.TransformPoint(src_gt[0], src_gt[3])

        (lrx, lry, lrz) = tx.TransformPoint(
            src_gt[0] + src_gt[1]*ndvi_ds.RasterXSize,
            src_gt[3] + src_gt[5]*ndvi_ds.RasterYSize)

        # new geotransform
        dst_gt = (ulx, pixel_xsize, src_gt[2],
                    uly, src_gt[4], -pixel_ysize)

        # -----------------

        # create new raster
        driver = gdal.GetDriverByName("COG")
        out_ds = driver.Create(
            output_path,
            int((lrx - ulx)/pixel_xsize),
            int((uly - lry)/pixel_ysize),
            1,
            gdal.GDT_Int16
        )

        # set transform and projection
        out_ds.SetGeoTransform(dst_gt)
        out_ds.SetProjection(dst_proj.ExportToWkt())

        out_band = out_ds.GetRasterBand(1)
        out_band.WriteArray(ndvi_array)
        out_band.SetNoDataValue(-9999)

        # complete write
        out_ds = None

        # close out datasets
        hdf_ds = None
        ndvi_ds = None


    def process_monthly_data(self, year_month, month_files, month_path):
        logger = self.get_logger()
        logger.info(f"Processing Month {year_month}")
        data, meta = self.aggregate_rasters(file_list=month_files, method="max")
        self.write_raster(month_path, data, meta)


    def process_yearly_data(self, year, year_files, year_path):
        logger = self.get_logger()
        print ("Processing Year {year}")
        data, meta = self.aggregate_rasters(file_list=year_files, method="mean")
        self.write_raster(year_path, data, meta)


    def aggregate_rasters(self, file_list, method="mean"):
        """
        Aggregate multiple rasters

        Aggregates multiple rasters with same features (dimensions, transform,
        pixel size, etc.) and creates single layer using aggregation method
        specified.

        Supported methods: mean (default), max, min, sum

        Arguments
            file_list (list): list of file paths for rasters to be aggregated
            method (str): method used for aggregation

        Return
            result: rasterio Raster instance
        """
        store = None
        for ix, file_path in enumerate(file_list):

            try:
                raster = rasterio.open(file_path)
            except:
                print (f"Could not include file in aggregation ({str(file_path)})")
                continue

            active = raster.read(masked=True)

            if store is None:
                store = active.copy()

            else:
                # make sure dimensions match
                if active.shape != store.shape:
                    raise Exception("Dimensions of rasters do not match")

                if method == "max":
                    store = np.ma.array((store, active)).max(axis=0)

                    # non masked array alternatives
                    # store = np.maximum.reduce([store, active])
                    # store = np.vstack([store, active]).max(axis=0)

                elif method == "mean":
                    if ix == 1:
                        weights = (~store.mask).astype(int)

                    store = np.ma.average(np.ma.array((store, active)), axis=0, weights=[weights, (~active.mask).astype(int)])
                    weights += (~active.mask).astype(int)

                elif method == "min":
                    store = np.ma.array((store, active)).min(axis=0)

                elif method == "sum":
                    store = np.ma.array((store, active)).sum(axis=0)

                else:
                    raise Exception("Invalid method")

        store = store.filled(raster.nodata)
        return store, raster.profile


    def write_raster(self, path, data, meta):
        make_dir(os.path.dirname(path))
        meta["dtype"] = data.dtype
        with rasterio.open(path, "w", **meta) as result:
            try:
                result.write(data)
            except:
                print(path)
                print(meta)
                print(data.shape)
                raise


    def main(self):

        # Build download list
        raw_file_list = self.run_tasks(self.build_sensor_download_list, [[s] for s in self.sensors])

        # We have a list of lists (from each sensor), merge them into one
        file_list = [i for i in chain(*raw_file_list.results())]

        # Extract list of files to download from file_list
        download_list = [i[1] for i in file_list if i[0]]

        # Download data
        if len(download_list) > 0:
            self.run_tasks(self.download, download_list).results()

        # Make a list of all daily files, regardless of how the downloads went
        day_files = [i[1][1] for i in file_list]

        # Build day dataframe
        day_df = self.build_process_list(day_files)

        # build month dataframe

        # Using pandas "named aggregation" to make ensure predictable column names in output.
        # See bottom of this page:
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.DataFrameGroupBy.aggregate.html
        # see also https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html#groupby-aggregate-named
        month_df = day_df[["output_path", "year", "year_month"]].groupby("year_month", as_index=False).aggregate(
            day_path_list = pd.NamedAgg(column="output_path",   aggfunc=lambda x: tuple(x)),
            count =         pd.NamedAgg(column="output_path",   aggfunc="count"),
            year =          pd.NamedAgg(column="year",          aggfunc="last")
        )

        minimum_days_in_month = 20

        month_df = month_df.loc[month_df["count"] >= minimum_days_in_month]

        month_df["output_path"] = month_df.apply(
            lambda x: os.path.join(self.output_dir, "monthly/avhrr_ndvi_v5_{}.tif".format(x["year_month"])), axis=1
        )

        # build year dataframe
        year_df = month_df[["output_path", "year"]].groupby("year", as_index=False).aggregate({
            "output_path": [lambda x: tuple(x), "count"]
        })
        year_df.columns = ["year", "month_path_list", "count"]


        year_df["output_path"] = year_df["year"].apply(
            lambda x: (self.output_dir / f"yearly/avhrr_ndvi_v5_{x}.tif").as_posix()
        )

        # Make _qlist arrays, which are handled by prep_xxx_data functions as lists of tasks

        day_qlist = []
        for _, row in day_df.iterrows():
            day_qlist.append([row["input_path"], row["output_path"]])

        month_qlist = []
        for _, row in month_df.iterrows():
            month_qlist.append([row["year_month"], row["day_path_list"], row["output_path"]])

        year_qlist = []
        for _, row in year_df.iterrows():
            year_qlist.append([row["year"], row["month_path_list"], row["output_path"]])

        if "daily" in self.build_list:
            os.makedirs(self.output_dir / "daily", exist_ok=True)
            self.run_tasks(self.process_daily_data, day_qlist)

        if "monthly" in self.build_list:
            os.makedirs(self.output_dir / "monthly", exist_ok=True)
            self.run_tasks(self.process_monthly_data, month_qlist)

        if "yearly" in self.build_list:
            os.makedirs(self.output_dir / "yearly", exist_ok=True)
            self.run_tasks(self.process_yearly_data, year_qlist)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "token": config["main"]["token"],
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "validate_download": config["main"].getboolean("validate_download"),
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

    class_instance = LTDR_NDVI(config_dict["token"], config_dict["years"], config_dict["raw_dir"], config_dict["output_dir"], config_dict["overwrite_download"], config_dict["validate_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
