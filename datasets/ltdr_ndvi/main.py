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

import hashlib
import json
import os
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import List, Tuple, Type, Union
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import rasterio
import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from rasterio.crs import CRS


class LTDR_NDVI_Configuration(BaseDatasetConfiguration):
    token: str
    data_num: int
    years: List[int]
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    validate_download: bool
    overwrite_processing: bool


class LTDR_NDVI(Dataset):
    name = "Long-term Data Record NDVI"

    def __init__(
        self,
        config: LTDR_NDVI_Configuration,
    ):
        self.build_list = ["daily", "monthly", "yearly"]

        self.auth_headers = {"Authorization": f"Bearer {config.token}"}

        self.years = config.years

        # TODO: warn if raw_dir already points to a directory named [data_num], it's probably one too deep
        self.raw_dir = Path(config.raw_dir) / str(config.data_num)
        self.output_dir = Path(config.output_dir)

        self.overwrite_download = config.overwrite_download
        self.validate_download = config.validate_download
        self.overwrite_processing = config.overwrite_processing

        self.dataset_url = f"https://ladsweb.modaps.eosdis.nasa.gov/api/v2/content/details/allData/{config.data_num}/"

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
                        dst = (
                            self.raw_dir
                            / sensor
                            / year_details["name"]
                            / day_details["name"]
                            / file_detail["name"]
                        )
                        # if file is already downloaded, and we aren't in overwrite mode
                        if dst.exists() and not self.overwrite_download:
                            if self.validate_download:
                                if validate(dst, file_detail["md5sum"]):
                                    logger.info(f"File validated: {dst.as_posix()}")
                                    download_list.append(
                                        (False, (day_download_url, dst))
                                    )
                                else:
                                    logger.info(
                                        f"File validation failed, queuing for download: {dst.as_posix()}"
                                    )
                                    download_list.append(
                                        (True, (day_download_url, dst))
                                    )
                            else:
                                logger.info(f"File exists, skipping: {dst.as_posix()}")
                                download_list.append((False, (day_download_url, dst)))
                        else:
                            logger.info(f"Queuing for download: {day_download_url}")
                            download_list.append((True, (day_download_url, dst)))
        return download_list

    def download(self, src_url: str, final_dst_path: Union[str, os.PathLike]) -> None:
        logger = self.get_logger()
        logger.info(f"Downloading {str(final_dst_path)}...")
        with requests.get(src_url, headers=self.auth_headers, stream=True) as src:
            src.raise_for_status()
            with self.tmp_to_dst_file(final_dst_path, make_dst_dir=True) as dst_path:
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
            "year_deny": ["2019"],
        }

        df_dict_list = []

        for input_path in downloaded_files:
            items = input_path.stem.split(".")
            year = items[1][1:5]
            day = items[1][5:8]
            sensor = items[2]
            month = "{0:02d}".format(datetime.strptime(f"{year}+{day}", "%Y+%j").month)
            output_path = (
                self.output_dir / "daily" / f"avhrr_ndvi_v5_{sensor}_{year}_{day}.tif"
            )
            df_dict_list.append(
                {
                    "input_path": input_path,
                    "sensor": sensor,
                    "year": year,
                    "month": month,
                    "day": day,
                    "year_month": year + "_" + month,
                    "year_day": year + "_" + day,
                    "output_path": output_path,
                }
            )

        df = pd.DataFrame(df_dict_list).sort_values(by=["input_path"])

        # df = df.drop_duplicates(subset="year_day", take_last=True)
        sensors = sorted(list(set(df["sensor"])))
        years = sorted(list(set(df["year"])))
        filter_sensors = None
        if ops["use_sensor_accept"]:
            filter_sensors = [i for i in sensors if i in ops["sensor_accept"]]
        elif ops["use_sensor_deny"]:
            filter_sensors = [i for i in sensors if i not in ops["sensor_deny"]]
        if filter_sensors:
            df = df.loc[df["sensor"].isin(filter_sensors)]
        filter_years = None
        if ops["use_year_accept"]:
            filter_years = [i for i in years if i in ops["year_accept"]]
        elif ops["use_year_deny"]:
            filter_years = [i for i in years if i not in ops["year_deny"]]
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

    def process_daily_data(self, src, output_path):
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

        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Skipping day, already processed: {sensor} {year} {day}")
        else:
            logger.info(f"Processing day: {sensor} {year} {day}")

            # list of qa fields and bit numbers
            # https://ltdr.modaps.eosdis.nasa.gov/ltdr/docs/AVHRR_LTDR_V5_Document.pdf

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
                0: "Unused",
            }

            # qa_mask_vals = [15, 9, 8, 6, 4, 3, 2, 1]
            qa_mask_vals = [15, 9, 8, 1]

            ndvi_gdal_path = f'HDF4_EOS:EOS_GRID:"{src.as_posix()}":Grid:NDVI'
            qa_gdal_path = f'HDF4_EOS:EOS_GRID:"{src.as_posix()}":Grid:QA'

            # open data subdataset
            with rasterio.open(ndvi_gdal_path) as ndvi_src:
                ndvi_array = ndvi_src.read(1)

                # open quality assurance subdataset
                with rasterio.open(qa_gdal_path) as qa_src:
                    qa_array = qa_src.read(1)

                    # create mask array using our chosen mask values
                    qa_mask = self.create_mask(qa_array, qa_mask_vals)

                    # apply mask to dataset
                    ndvi_array[qa_mask] = -9999

                ndvi_array[np.where((ndvi_array < 0) & (ndvi_array > -9999))] = 0
                ndvi_array[np.where(ndvi_array > 10000)] = 10000

                profile = {
                    "count": 1,
                    "driver": "COG",
                    "compress": "LZW",
                    "dtype": "int16",
                    "nodata": -9999,
                    "height": 3600,
                    "width": 7200,
                    "crs": CRS.from_epsg(4326),
                    "transform": ndvi_src.transform,
                }

                with self.tmp_to_dst_file(output_path) as dst_path:
                    with rasterio.open(dst_path, "w", **profile) as dst:
                        # for some reason rasterio raises an exception if we don't specify that there is one index
                        dst.write(ndvi_array, indexes=1)

    def process_monthly_data(self, year_month, month_files, month_path):
        logger = self.get_logger()
        if os.path.exists(month_path) and not self.overwrite_processing:
            logger.info(f"Skipping month, already processed: {year_month}")
        else:
            logger.info(f"Processing month: {year_month}")
            data, meta = self.aggregate_rasters(file_list=month_files, method="max")
            self.write_raster(month_path, data, meta)

    def process_yearly_data(self, year, year_files, year_path):
        logger = self.get_logger()
        if os.path.exists(year_path) and not self.overwrite_processing:
            logger.info(f"Skipping year, already processed: {year}")
        else:
            logger.info(f"Processing year: {year}")
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
        logger = self.get_logger()
        store = None
        for ix, file_path in enumerate(file_list):
            try:
                raster = rasterio.open(file_path)
            except:
                logger.error(
                    f"Could not include file in aggregation ({str(file_path)})"
                )
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

                    store = np.ma.average(
                        np.ma.array((store, active)),
                        axis=0,
                        weights=[weights, (~active.mask).astype(int)],
                    )
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
        logger = self.get_logger()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        meta["dtype"] = data.dtype
        with self.tmp_to_dst_file(path) as write_path:
            with rasterio.open(write_path, "w", **meta) as result:
                try:
                    result.write(data)
                except:
                    logger.exception("Error writing raster to {path}")

    def main(self):
        # Build download list
        raw_file_list = self.run_tasks(
            self.build_sensor_download_list, [[s] for s in self.sensors]
        )

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
        month_df = (
            day_df[["output_path", "year", "year_month"]]
            .groupby("year_month", as_index=False)
            .aggregate(
                day_path_list=pd.NamedAgg(
                    column="output_path", aggfunc=lambda x: tuple(x)
                ),
                count=pd.NamedAgg(column="output_path", aggfunc="count"),
                year=pd.NamedAgg(column="year", aggfunc="last"),
            )
        )

        minimum_days_in_month = 20

        month_df = month_df.loc[month_df["count"] >= minimum_days_in_month]

        month_df["output_path"] = month_df.apply(
            lambda x: (
                self.output_dir / "monthly/avhrr_ndvi_v5_{}.tif".format(x["year_month"])
            ).as_posix(),
            axis=1,
        )

        # build year dataframe
        year_df = (
            month_df[["output_path", "year"]]
            .groupby("year", as_index=False)
            .aggregate({"output_path": [lambda x: tuple(x), "count"]})
        )
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
            month_qlist.append(
                [row["year_month"], row["day_path_list"], row["output_path"]]
            )

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


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def ltdr_ndvi(config: LTDR_NDVI_Configuration):
        LTDR_NDVI(config).run(config.run)


if __name__ == "__main__":
    config = get_config(LTDR_NDVI_Configuration)
    LTDR_NDVI(config).run(config.run)
