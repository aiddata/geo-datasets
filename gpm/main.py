import os
import warnings
from pathlib import Path
from datetime import datetime
from typing import List, Literal
from configparser import ConfigParser

from pathlib import Path
from ftplib import FTP, FTP_TLS

import rasterio
import numpy as np

from data_manager import Dataset


def aggregate_rasters(file_list, method="mean"):
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
            print("Could not include file in aggregation ({0})".format(file_path))
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
    return store, dict(raster.meta)


def export_raster(data, path, meta, **kwargs):
    """
    Export raster array to geotiff
    """

    if not isinstance(meta, dict):
        raise ValueError("meta must be a dictionary")

    if "dtype" in meta:
        if meta["dtype"] != data.dtype:
            warnings.warn(f"Dtype specified by meta({meta['dtype']}) does not match data dtype ({data.dtype}). Adjusting data dtype to match meta.")
        data = data.astype(meta["dtype"])
    else:
        meta["dtype"] = data.dtype

    default_meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'driver': 'GTiff',
        'compress': 'lzw',
        'nodata': -9999,
        'height': data.shape[1],
        'width': data.shape[2],
    }

    for k, v in default_meta.items():
        if k not in meta:
            if "quiet" not in kwargs or kwargs["quiet"] == False:
                print(f"Value for `{k}` not in meta provided. Using default value ({v})")
            meta[k] = v

    # write geotif file
    with rasterio.open(path, "w", **meta) as dst:
        dst.write(data)


class GPM(Dataset):
    name = "GPM"

    def __init__(self,
                 raw_dir: str,
                 output_dir: str,
                 email: dict,
                 version: str,
                 years: list,
                 year_agg_method: str,
                 overwrite_downloads: bool,
                 verify_existing_downloads: bool,
                 overwrite_processing: bool,):

        self.version = version
        self.year_agg_method = year_agg_method
        self.years = [int(y) for y in years]
        self.email = email

        self.raw_dir = Path(raw_dir) / self.version
        self.output_dir = Path(output_dir) / self.version

        self.monthly_dir = self.output_dir / "monthly"
        self.yearly_dir = self.output_dir / "yearly" / year_agg_method

        # skip existing files while downloading?
        self.overwrite_downloads = overwrite_downloads

        # verify existing files' hashes while downloading?
        self.verify_existing_downloads = verify_existing_downloads

        self.overwrite_processing = overwrite_processing

        self.year_mask = "gpm_precipitation_YYYY.tif"
        self.year_sep = "_"
        self.year_loc = 2


    def init_ftps(self):
        ftps = FTP_TLS()
        ftps.connect("arthurhouftps.pps.eosdis.nasa.gov")
        ftps.login(user=self.email, passwd=self.email)
        ftps.cwd('gpmdata')
        return ftps


    def build_download_list(self):
        """
        data folder format:
            ftp://arthurhou.pps.eosdis.nasa.gov/gpmdata/2014/05/03/imerg/
        file name:
            3B-MO.MS.MRG.3IMERG.20140501-S000000-E235959.05.V05B.HDF5
            3B-HHR.MS.MRG.3IMERG.20140501-S233000-E235959.1410.V05B.HDF5
        """
        logger = self.get_logger()

        ftps = self.init_ftps()
        rootdir = Path(ftps.pwd())

        months = [i.zfill(2) for i in map(str, range(1,13))]

        # monthly data is saved in the first date's folder
        dates = ['01']

        download_list = []

        for year in self.years:
            for month in months:
                # on ftps: geotiff data in "gis" dir, hdf5 in "imerg" dir
                filepath = str(rootdir / str(year) / str(month) / dates[0] / 'gis')

                try:
                    ftps.cwd(filepath)

                    filelist = ftps.nlst()

                    for file in filelist:
                        if "3B-MO" in file and file.endswith(".tif"):
                            logger.info(file)

                            if self.version.upper() not in file:
                                logger.warn(f"version mismatch: {self.version} not in {file}")

                            local_filename = self.raw_dir / file

                            download_list.append((local_filename, filepath, file, year, month))
                except:
                    logger.info("no data for: {0} {1}".format(year, month))

            return download_list


    def download_gpm(self, local_filename, filepath, file, year, month):
        logger = self.get_logger()

        local_filename.parent.mkdir(parents=True, exist_ok=True)

        if os.path.isfile(local_filename):
            logger.info(f"File already exists: {local_filename}")
            if not self.overwrite_downloads:
                logger.info("Skipping download")
                return

        ftps = self.init_ftps()
        ftps.cwd(filepath)

        try:
            lf = open(local_filename, "wb")
            ftps.retrbinary("RETR " + file, lf.write)
            lf.close()
        except:
            logger.error("Cannot download file: {}".format(file))


    def run_monthly_data(self, f):
        input_file = self.raw_dir / f

        date = f.split(".")[4].split("-")[0]
        year = date[:4]
        month = str(date[4:6])
        name = self.year_mask.replace("YYYY", f"{year}_{month}")

        print("Processing {}".format(name))

        output_file = self.monthly_dir / name

        src = rasterio.open(input_file)
        data = src.read(1)
        meta = {
            "transform": src.transform,
            "dtype": str(data.dtype),
            "nodata": "9999",
        }
        export_raster(np.array([data]), output_file, meta)


    def build_year_tasks(self):
        year_months = {}
        month_files = [str(i) for i in self.monthly_dir.iterdir() if str(i).endswith('.tif')]

        for mfile in month_files:
            # year associated with month
            myear = mfile.split(self.year_sep)[self.year_loc]
            if myear not in year_months:
                year_months[myear] = list()

            year_months[myear].append(self.monthly_dir / mfile)

        year_qlist = [
            (year_group, month_paths) for year_group, month_paths in year_months.items()
            if len(month_paths) == 12
        ]

        return year_qlist


    def run_yearly_data(self, year, year_files, **kwargs):
        # year, year_files = task
        data, meta = aggregate_rasters(file_list=year_files, method=self.year_agg_method)
        year_path = Path(self.yearly_dir) / self.year_mask.replace("YYYY", str(year))
        export_raster(data, year_path, meta)


    def main(self):

        logger = self.get_logger()

        logger.info("Building initial download list")
        dl_task_list = self.build_download_list()

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        if len(dl_task_list) > 0:
            logger.info("Downloading Data")
            dl_run = self.run_tasks(self.download_gpm, dl_task_list, force_sequential=True)
            self.log_run(dl_run)
        else:
            logger.info("Skipping download, no files queued for download")


        logger.info("Preparing Monthly Data")
        monthly_task_list = sorted([f for f in self.raw_dir.iterdir() if str(f).endswith("tif")])
        monthly_task_list = [(str(f),) for f in monthly_task_list]
        self.monthly_dir.mkdir(parents=True, exist_ok=True)
        month_run = self.run_tasks(self.run_monthly_data, monthly_task_list, force_sequential=True)
        self.log_run(month_run)

        logger.info("Running Yearly Aggregations")
        yearly_task_list = self.build_year_tasks()
        self.yearly_dir.mkdir(parents=True, exist_ok=True)
        year_run = self.run_tasks(self.run_yearly_data, yearly_task_list, force_sequential=True)
        self.log_run(year_run)



def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "version": config["main"]["version"],
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "year_agg_method": config["main"]["year_agg_method"],
        "email": config["main"]["email"],
        "overwrite_downloads": config["main"].getboolean("overwrite_downloads"),
        "verify_existing_downloads": config["main"].getboolean("verify_existing_downloads"),
        "overwrite_processing": config["main"].getboolean("overwrite_processing"),
        "backend": config["run"]["backend"],
        "task_runner": config["run"]["task_runner"],
        "run_parallel": config["run"].getboolean("run_parallel"),
        "max_workers": int(config["run"]["max_workers"]),
        "log_dir": Path(config["main"]["raw_dir"]) / "logs",
    }


if __name__ == "__main__":

    config_dict = get_config_dict()

    log_dir = config_dict["log_dir"]
    timestamp = datetime.today()
    time_format_str: str="%Y_%m_%d_%H_%M"
    time_str = timestamp.strftime(time_format_str)
    timestamp_log_dir = Path(log_dir) / time_str
    timestamp_log_dir.mkdir(parents=True, exist_ok=True)


    class_instance = GPM(config_dict["raw_dir"], config_dict["output_dir"], config_dict["email"], config_dict["version"], config_dict["years"], config_dict["year_agg_method"], config_dict["overwrite_downloads"], config_dict["verify_existing_downloads"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)

else:
    try:
        from prefect import flow
    except:
        pass
    else:
        config_file = "gpm/config.ini"
        config = ConfigParser()
        config.read(config_file)

        from main import GPM

        tmp_dir = Path(os.getcwd()) / config["github"]["directory"]

        @flow
        def gpm(
            raw_dir: str,
            output_dir: str,
            email: str,
            version: str,
            years: List[int],
            year_agg_method: str,
            overwrite_downloads: bool,
            verify_existing_downloads: bool,
            overwrite_processing: bool,
            backend: Literal["local", "mpi", "prefect"],
            task_runner: Literal["sequential", "concurrent", "dask", "hpc", "kubernetes"],
            run_parallel: bool,
            max_workers: int,
            log_dir: str):

            timestamp = datetime.today()
            time_str = timestamp.strftime("%Y_%m_%d_%H_%M")
            timestamp_log_dir = Path(log_dir) / time_str
            timestamp_log_dir.mkdir(parents=True, exist_ok=True)

            cluster = "vortex"


            hpc_cluster_kwargs = {
                "shebang": "#!/bin/tcsh",
                "resource_spec": "nodes=1:c18a:ppn=12",
                "walltime": "4:00:00",
                "cores": 3,
                "processes": 3,
                "memory": "30GB",
                "interface": "ib0",
                "job_extra_directives": [
                    "-j oe",
                ],
                "job_script_prologue": [
                    "source /usr/local/anaconda3-2021.05/etc/profile.d/conda.csh",
                    "module load anaconda3/2021.05",
                    "conda activate geodata38",
                    f"cd {tmp_dir}",
                ],
                "log_directory": str(timestamp_log_dir),
            }


            class_instance = GPM(raw_dir=raw_dir, output_dir=output_dir, email=email, version=version, years=years, year_agg_method=year_agg_method, overwrite_downloads=overwrite_downloads, verify_existing_downloads=verify_existing_downloads, overwrite_processing=overwrite_processing)


            if task_runner != 'hpc':
                os.chdir(tmp_dir)
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir)
            else:
                class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=timestamp_log_dir, cluster=cluster, cluster_kwargs=hpc_cluster_kwargs)
