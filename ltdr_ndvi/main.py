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
import csv
import sys
import errno
import shutil
from io import StringIO
from pathlib import Path
from datetime import datetime
from collections import OrderedDict
from configparser import ConfigParser

import rasterio
import numpy as np
import pandas as pd
from osgeo import gdal, osr


sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class LTDR_NDVI(Dataset):

    name = "Long-term Data Record NDVI"

    def __init__(self, app_key:str, years, raw_dir, output_dir, overwrite_download: bool, overwrite_processing: bool):

        self.build_list = [
            "daily",
            "monthly",
            "yearly"
        ]

        self.app_key = app_key

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)

        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing

        """
        src_base = "/sciclone/aiddata10/REU/geo/raw/ltdr/LAADS/465"

        dst_base = "/sciclone/aiddata10/REU/geo/data/rasters/ltdr/avhrr_ndvi_v5"
        """

    def download(self):

        logger = self.get_logger()

        base_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/465/"

        sensors = [
            "N07_AVH13C1",
            "N09_AVH13C1",
            "N11_AVH13C1",
            "N14_AVH13C1",
            "N16_AVH13C1",
            "N18_AVH13C1",
            "N19_AVH13C1",
        ]

        # Adapted from https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/data-download-scripts/

        def geturl(url, token=None, out=None):
            headers = {"Authorization": "Bearer " + self.app_key }
            import ssl
            CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            from urllib.request import urlopen, Request, URLError, HTTPError
            try:
                fh = urlopen(Request(url, headers=headers), context=CTX)
                if out is None:
                    return fh.read().decode("utf-8")
                else:
                    shutil.copyfileobj(fh, out)
            except HTTPError as e:
                logger.exception(f"HTTP GET error code: {str(e.code())}")
                logger.exception(f"HTTP GET error message: {e.message}")
            except URLError as e:
                logger.exception(f"Failed to make request: {e.reason}")
            return None


        for s in sensors:
            src = base_url + s
            logger.info(f"Downloading {src}")

            os.makedirs(self.raw_dir, exist_ok=True)

            files = [ f for f in csv.DictReader(StringIO(geturl(f"{src}.csv")), skipinitialspace=True) ]

            for f in files:
                # filesize of 0 to indicates a directory
                filesize = int(f["size"])
                path = self.raw_dir / f["name"]
                url = src + f["name"]
                if filesize == 0:
                    os.makedirs(path, exist_ok=True)
                    sync(src + "/" + f["name"], path)
                else:
                    if path.exists() and not self.overwrite_download:
                        logger.info(f"Skipping, already downloaded: {path.as_posix()}")
                    else:
                        logger.info(f"Downloading: {path.as_posix()}")
                        with open(path, 'w+b') as fh:
                            geturl(url)



    def build_data_list(self, input_base, output_base):
        output_base = Path(output_base)

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

        f = []
        # find all .hdf files under input_base in filesystem
        for root, dirs, files in os.walk(input_base):
            for file in files:
                if file.endswith(".hdf"):
                    # ...and add them to the f array
                    f.append(os.path.join(root, file))
        df_dict_list = []

        for input_path in f:
            input_path = Path(input_path)
            items = input_path.stem.split(".")
            year = items[1][1:5]
            day = items[1][5:8]
            sensor = items[2]
            month = "{0:02d}".format(datetime.strptime(f"{year}+{day}", "%Y+%j").month)
            output_path = output_base / f"daily/avhrr_ndvi_v5_{sensor}_{year}_{day}.tif"
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


    def prep_daily_data(self, src, dst):
        logger = self.get_logger()

        year = src.name.split(".")[1][1:5]
        day = src.name.split(".")[1][5:8]
        sensor = src.name.split(".")[2]

        logger.info(f"Processing Day {sensor} {year} {day}")
        self.process_daily_data(src.as_posix(), dst)


    def prep_monthly_data(self, year_month, month_files, month_path):
        logger = self.get_logger()
        logger.info(f"Processing Month {year_month}")
        data, meta = self.aggregate_rasters(file_list=month_files, method="max")
        self.write_raster(month_path, data, meta)


    def prep_yearly_data(self, year, year_files, year_path):
        logger = self.get_logger()
        print ("Processing Year {year}")
        data, meta = self.aggregate_rasters(file_list=year_files, method="mean")
        self.write_raster(year_path, data, meta)


    def create_mask(self, qa_array, mask_vals):
        qa_mask_vals = [abs(x - 15) for x in mask_vals]
        mask_bin_array = [0] * 16
        for x in qa_mask_vals:
            mask_bin_array[x] = 1
        mask_bin = int("".join(map(str, mask_bin_array)), 2)

        flag = lambda i: (i & 65535 & mask_bin) != 0

        qa_mask = pd.DataFrame(qa_array).applymap(flag).to_numpy()
        return qa_mask


    def process_daily_data(self, input_path, output_path):
        """Process input raster and create output in output directory

        Unpack NDVI subdataset from a HDF container
        Reproject to EPSG:4326
        Set values <0 (other than nodata) to 0
        Write to GeoTiff

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

        breakpoint()

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



    def aggregate_rasters(self, file_list, method="mean"):
        """Aggregate multiple rasters

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
        # Build day, month, year dataframes

        # build day dataframe
        day_df = self.build_data_list(self.raw_dir, self.output_dir)

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
            self.run_tasks(self.prep_daily_data, day_qlist)

        if "monthly" in self.build_list:
            os.makedirs(self.output_dir / "monthly", exist_ok=True)
            self.run_tasks(self.prep_monthly_data, month_qlist)

        if "yearly" in self.build_list:
            os.makedirs(self.output_dir / "yearly", exist_ok=True)
            self.run_tasks(self.prep_yearly_data, year_qlist)


def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "app_key": config["main"]["app_key"],
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
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

    class_instance = LTDR_NDVI(config_dict["app_key"], config_dict["years"], config_dict["raw_dir"], config_dict["output_dir"], config_dict["overwrite_download"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
