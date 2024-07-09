import glob
import os
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import List, Literal, Union

import h5py
import numpy as np
import pandas as pd
import rasterio
import requests
from affine import Affine
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from scipy.interpolate import griddata

from utility import file_exists, find_files, get_current_timestamp


class SessionWithHeaderRedirection(requests.Session):
    """
    overriding requests.Session.rebuild_auth to mantain headers when redirected
    from: https://wiki.earthdata.nasa.gov/display/EL/How+To+Access+Data+With+Python
    """

    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        """
        Overrides from the library to keep headers when redirected to or from
        the NASA auth host.
        """

        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (
                (original_parsed.hostname != redirect_parsed.hostname)
                and redirect_parsed.hostname != self.AUTH_HOST
                and original_parsed.hostname != self.AUTH_HOST
            ):
                del headers["Authorization"]
        return


class OCO2Configuration(BaseDatasetConfiguration):
    data_url: str
    username: str
    password: str
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_processing: bool
    year_list: List[int]
    interp_method: str
    run_a: bool
    run_b: bool
    run_c: bool
    run_d: bool
    run_e: bool
    run_f: bool
    run_g: bool


class OCO2(Dataset):
    name = "OCO-2 Carbon Dioxide"

    def __init__(
        self,
        config: OCO2Configuration,
    ):
        self.timestamp = get_current_timestamp("%Y_%m_%d_%H_%M")

        self.interp_method = config.interp_method
        self.data_url = config.data_url
        self.username = config.username
        self.password = config.password
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.year_list = config.year_list
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

        self.day_dir = os.path.join(self.output_dir, "day")
        self.month_dir = os.path.join(self.output_dir, "month")
        self.month_grid_dir = os.path.join(self.output_dir, "month_grid")
        self.month_interp_dir = os.path.join(self.output_dir, "month_interp")
        self.year_dir = os.path.join(self.output_dir, "year")
        self.year_grid_dir = os.path.join(self.output_dir, "year_grid")
        self.year_interp_dir = os.path.join(self.output_dir, "year_interp")

        self.run_a = config.run_a
        self.run_b = config.run_b
        self.run_c = config.run_c
        self.run_d = config.run_d
        self.run_e = config.run_e
        self.run_f = config.run_f
        self.run_g = config.run_g

        os.makedirs(self.day_dir, exist_ok=True)
        os.makedirs(self.month_dir, exist_ok=True)
        os.makedirs(self.year_dir, exist_ok=True)
        os.makedirs(self.month_grid_dir, exist_ok=True)
        os.makedirs(self.month_interp_dir, exist_ok=True)
        os.makedirs(self.year_grid_dir, exist_ok=True)
        os.makedirs(self.year_interp_dir, exist_ok=True)

        os.makedirs(os.path.join(self.raw_dir, "results"), exist_ok=True)

    def test_connection(self) -> None:
        """Verify that we can connect to the given data URL"""
        test_request = requests.get(self.data_url)
        test_request.raise_for_status()

    def manage_download(self, url, local_filename):
        """download individual file using session created

        this needs to be a standalone function rather than a method
        of SessionWithHeaderRedirection because we need to be able
        to pass it to our mpi4py map function
        """

        logger = self.get_logger()

        overwrite = False
        # max_attempts = 3
        # md5sum = get_md5sum_from_xml_url(f"{url}.xml", "checksumvalue")
        if (
            file_exists(local_filename)
            and not overwrite
            # and calc_md5sum(local_filename) == md5sum
        ):
            logger.info(f"File already exists: {local_filename}. Skipping...")
        else:
            session = SessionWithHeaderRedirection(self.username, self.password)

            with session.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)

            logger.info(f"Downloaded: {url} -> {local_filename}")
            # attempts = 1
            # download_file(url, local_filename)
            # # confirm that file was downloaded correctly
            # while calc_md5sum(local_filename) != md5sum and attempts <= max_attempts:
            #     download_file(url, local_filename)
            #     attempts += 1
            # if calc_md5sum(local_filename) != md5sum:
            #     raise ValueError(f"Invalid md5sum for file downloaded from {url}")
            #     # return (2, "Invalid md5sum", url)
            # else:
            #     return
            #     # return (0, "Success", url)

    @staticmethod
    def drop_existing_files(file_tuples, overwrite=False):
        """
        Drop file tuples if output file exists and overwrite not True

        Designed to look only at the second item of a tuple because
        all qlists are tuples of length 2 where the second item is the
        output path.
        """
        return [f for f in file_tuples if overwrite or not file_exists(f[1])]

    def download_data(self):
        print("Preparing data download")

        year_file_list = []
        for year in self.year_list:
            year_url = os.path.join(self.data_url, str(year))
            print(f"data_url is {self.data_url}")
            print(f"year is {year}")
            print(f"year_url is {year_url}")
            year_files = find_files(year_url, ".nc4")
            year_file_list.extend(year_files)

        df = pd.DataFrame({"raw_url": year_file_list})

        # use basename from url to create local filename
        df["output"] = df["raw_url"].apply(
            lambda x: os.path.join(self.raw_dir, os.path.basename(x))
        )

        os.makedirs(self.raw_dir, exist_ok=True)

        # generate list of tasks to iterate over
        flist = list(zip(df["raw_url"], df["output"]))

        print("Running data download")
        return self.run_tasks(self.manage_download, flist)

    def convert_daily(self, input_path, output_path):
        """
        Convert daily nc4 files to csv
        """
        logger = self.get_logger()

        output_path = Path(output_path)

        if not self.overwrite_processing and output_path.exists():
            logger.info(
                f"Daily conversion target {output_path} already exists, skipping..."
            )
        else:
            logger.info("Converting {}".format(output_path))
            with h5py.File(input_path, "r") as hdf_data:
                lon = list(hdf_data["longitude"])
                lat = list(hdf_data["latitude"])
                xco2 = list(hdf_data["xco2"])
                xco2_quality_flag = list(hdf_data["xco2_quality_flag"])
            point_list = list(zip(lon, lat, xco2, xco2_quality_flag))
            df = pd.DataFrame(
                point_list, columns=["lon", "lat", "xco2", "xco2_quality_flag"]
            )
            df.to_csv(output_path, index=False, encoding="utf-8")

    @staticmethod
    def read_csv(path):
        df = pd.read_csv(
            path, quotechar='"', na_values="", keep_default_na=False, encoding="utf-8"
        )
        return df

    def concat_data(self, flist, out_path):
        """
        Concat daily data csv to monthly data csv
        """
        df_list = [self.read_csv(f) for f in flist]
        out = pd.concat(df_list, axis=0, ignore_index=True)
        out.to_csv(out_path, index=False, encoding="utf-8")

    def concat_month(self, flist, out_path):
        logger = self.get_logger()

        if Path(out_path).exists() and not self.overwrite_processing:
            logger.info(
                f"Concatentation of month already exists at {out_path}, skipping..."
            )
        else:
            logger.info("Concat yearmonth {}".format(out_path))
            self.concat_data(flist, out_path)

    def concat_year(self, flist, out_path):
        logger = self.get_logger()

        if Path(out_path).exists() and not self.overwrite_processing:
            logger.info(
                f"Concatentation of year already exists at {out_path}, skipping..."
            )
        else:
            logger.info("Concat year {}".format(out_path))
            self.concat_data(flist, out_path)

    @staticmethod
    def round_to(value, interval):
        """round value to nearest interval of a decimal value
        e.g., every 0.25
        """
        if interval > 1:
            raise ValueError(
                "Must provide float less than (or equal to) 1 indicating interval to round to"
            )
        return round(value * (1 / interval)) * interval

    @staticmethod
    def lonlat(lon, lat, dlen):
        """
        Create unique id string combining latitude and longitude
        """
        str_lon = format(lon, "0.{}f".format(dlen))
        str_lat = format(lat, "0.{}f".format(dlen))
        lon_lat = "{}_{}".format(str_lon, str_lat)
        return lon_lat

    def agg_to_grid(self, input_path, output_path, rnd_interval=0.1):
        """aggregate coordinates to regular grid points"""
        decimal_places = len(str(rnd_interval).split(".")[1])
        df = self.read_csv(input_path)
        df = df.loc[df["xco2_quality_flag"] == 0].copy(deep=True)
        df["lon"] = df["lon"].apply(lambda z: self.round_to(z, rnd_interval))
        df["lat"] = df["lat"].apply(lambda z: self.round_to(z, rnd_interval))
        df["lonlat"] = df.apply(
            lambda z: self.lonlat(z["lon"], z["lat"], decimal_places), axis=1
        )
        agg_ops = {
            "xco2": "mean",
            "xco2_quality_flag": "count",
            "lon": "first",
            "lat": "first",
        }
        agg_df = df.groupby("lonlat", as_index=False).agg(agg_ops)
        agg_df.columns = [
            i.replace("xco2_quality_flag", "count") for i in agg_df.columns
        ]
        agg_df.to_csv(output_path, index=False, encoding="utf-8")

    def agg_to_grid_month(self, input_path, output_path):
        logger = self.get_logger()

        if Path(output_path).exists() and not self.overwrite_processing:
            logger.info(
                f"Aggregate to grid month already exists at {output_path}, skipping..."
            )
        else:
            logger.info("Agg {}".format(output_path))
            self.agg_to_grid(input_path, output_path)

    def agg_to_grid_year(self, input_path, output_path):
        logger = self.get_logger()

        if Path(output_path).exists() and not self.overwrite_processing:
            logger.info(
                f"Aggregate to grid year already exists at {output_path}, skipping..."
            )
        else:
            logger.info("Agg {}".format(output_path))
            self.agg_to_grid(input_path, output_path)

    def interpolate(
        self, input_path, output_path, rnd_interval=0.1, interp_method="linear"
    ):
        """interpolate
        https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.griddata.html#scipy.interpolate.griddata
        https://earthscience.stackexchange.com/questions/12057/how-to-interpolate-scattered-data-to-a-regular-grid-in-python
        """
        data = self.read_csv(input_path)
        # data coordinates and values
        x = data["lon"]
        y = data["lat"]
        z = data["xco2"]
        # target grid to interpolate to
        xi = np.arange(-180.0, 180.0 + rnd_interval, rnd_interval)
        yi = np.arange(90.0, -90.0 - rnd_interval, -rnd_interval)
        xi, yi = np.meshgrid(xi, yi)
        # interpolate
        zi = griddata((x, y), z, (xi, yi), method=interp_method)
        # prepare raster
        transform = Affine(rnd_interval, 0, -180.0, 0, -rnd_interval, 90.0)
        meta = {
            "count": 1,
            "crs": {"init": "epsg:4326"},
            "dtype": str(zi.dtype),
            "transform": transform,
            "driver": "GTiff",
            "height": zi.shape[0],
            "width": zi.shape[1],
        }
        raster_out = np.array([zi])
        with rasterio.open(output_path, "w", **meta) as dst:
            dst.write(raster_out)

    def interpolate_month(self, input_path, output_path):
        print("Interpolating {}".format(output_path))
        self.interpolate(input_path, output_path)

    def interpolate_year(self, input_path, output_path):
        print("Interpolating {}".format(output_path))
        self.interpolate(input_path, output_path)

    def output_results(self, tasks, results, stage):
        """Format and output results from running tasks

        All tasks are tuples with 2 items, where the input path or
        list of paths is the first item and the output path is the second item.
        """
        df = pd.DataFrame(tasks, columns=["input", "output"])

        # ---------
        # column name for join field in original df
        results_join_field_name = "output"
        # position of join field in each tuple in task list
        results_join_field_loc = 1
        # ---------

        # join download function results back to df
        results_df = pd.DataFrame(
            results, columns=["status", "message", results_join_field_name]
        )
        results_df[results_join_field_name] = results_df[results_join_field_name].apply(
            lambda x: x[results_join_field_loc]
        )

        output_df = df.merge(results_df, on=results_join_field_name, how="left")

        print("Results:")

        errors_df = output_df[output_df["status"] != 0]
        print(
            "\t{} errors found out of {} tasks".format(len(errors_df), len(output_df))
        )

        # output results to csv
        output_path = os.path.join(
            self.raw_dir, "results", f"data_prepare_{self.timestamp}_{stage}.csv"
        )
        output_df.to_csv(output_path, index=False)

    def main(self):
        # download data
        self.download_data()

        # prepare daily data
        input_list = glob.glob(os.path.join(self.raw_dir, "oco2_LtCO2_*.nc4"))
        output_list = [
            os.path.join(
                self.day_dir, "xco2_20{}.csv".format(os.path.basename(i).split("_")[2])
            )
            for i in input_list
        ]

        # netcdf daily path, csv daily path
        qlist_a = list(zip(input_list, output_list))

        if self.run_a:
            results_a = self.run_tasks(self.convert_daily, qlist_a)
            # self.output_results(qlist_a, results_a, "convert-daily")

        # concat all daily for each month
        qlist_b_dict = {}
        for _, i in qlist_a:
            yearmonth = os.path.basename(i).split("_")[1][0:6]
            if yearmonth not in qlist_b_dict:
                qlist_b_dict[yearmonth] = []
            qlist_b_dict[yearmonth].append(i)

        # list of daily csv paths, monthly csv path
        qlist_b = [
            (j, os.path.join(self.month_dir, "xco2_{}.csv".format(i)))
            for i, j in list(qlist_b_dict.items())
        ]

        if self.run_b:
            results_b = self.run_tasks(self.concat_month, qlist_b)
            # self.output_results(qlist_b, results_b, "concat-month")

        # concat all month for each year
        mlist = [i[1] for i in qlist_b]

        qlist_c_dict = {}
        for i in mlist:
            year = os.path.basename(i).split("_")[1][0:4]
            if year not in qlist_c_dict:
                qlist_c_dict[year] = []
            qlist_c_dict[year].append(i)

        # list of monthly csv paths, yearly csv path
        qlist_c = [
            (j, os.path.join(self.year_dir, "xco2_{}.csv".format(i)))
            for i, j in list(qlist_c_dict.items())
        ]

        if self.run_c:
            results_c = self.run_tasks(self.concat_year, qlist_c)
            # self.output_results(qlist_c, results_c, "concat-year")

        # agg monthly data to grid
        # monthly csv path, monthly grid csv path
        qlist_d = [(i, i.replace("month", "month_grid")) for i in mlist]

        if self.run_d:
            results_d = self.run_tasks(
                self.agg_to_grid_month,
                qlist_d,
            )
            # self.output_results(qlist_d, results_d, "agg-month")

        # interpolate month grid data to fill gaps
        # monthly grid csv path, monthly interpolation csv path
        qlist_e = [
            (
                j,
                os.path.join(
                    self.month_interp_dir,
                    "xco2_{}_{}.tif".format(
                        os.path.basename(j).split("_")[1][0:6], self.interp_method
                    ),
                ),
            )
            for _, j in qlist_d
        ]

        if self.run_e:
            results_e = self.run_tasks(
                self.interpolate_month,
                qlist_e,
            )
            # self.output_results(qlist_e, results_e, "interp-month")

        # agg yearly data to grid
        # yearly csv path, yearly grid csv path
        qlist_f = [(i[1], i[1].replace("year", "year_grid")) for i in qlist_c]

        if self.run_f:
            results_f = self.run_tasks(
                self.agg_to_grid_year,
                qlist_f,
            )
            # self.output_results(qlist_f, results_f, "agg-year")

        # interpolate year grid data to fill gaps
        # year grid csv path, year interpolation csv path
        qlist_g = [
            (
                j,
                os.path.join(
                    self.year_interp_dir,
                    "xco2_{}_{}.tif".format(
                        os.path.basename(j).split("_")[1][0:4], self.interp_method
                    ),
                ),
            )
            for _, j in qlist_f
        ]

        if self.run_g:
            results_g = self.run_tasks(
                self.interpolate_year,
                qlist_g,
            )
            # self.output_results(qlist_g, results_g, "interp-year")


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def oco2(config: OCO2Configuration):
        OCO2(config).run(config.run)


if __name__ == "__main__":
    config = get_config(OCO2Configuration)
    OCO2(config).run(config.run)
