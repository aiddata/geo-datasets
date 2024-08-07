"""
Python 3.8

Be sure to create (if needed) and initialize the Conda environment before running this script
You can do this by running create_env.sh to install the environment defined in environment.yml
`./create_env.sh`
`conda activate geodata-cru`

Alternatively, you can define the conda environment yourself. Here is what that might look like:
`conda create -n geodata-cru python=3.8 rasterio netcdf4`
`conda activate geodata-cru`

Make sure to review all variables (static file names, years) when using this script for a new version of cru data

The netcdf to geotiff conversion part of this script is fairly quick running in serial (<10minutes)
but would be easy to adapt for basic parellelization.

    Suggested parallelization steps would be to just add the in_path variable for each
    cru var to an instance of the band_temporal_list and combine all of the resulting lists.
    The trios in this list could then each be passed to the extract_layer function using any
    parellization map function.

The yearly aggregation is parellized using the multiprocessing package and only requires access
to multiple cores. This can be run on SciClone's Hima nodes using the following command to start a job:
`qsub -I -l nodes=1:hima:nogpu:ppn=64 -l walltime=24:00:00`

"""

import gzip
from pathlib import Path
from urllib import parse, request

import numpy as np
import rasterio
from data_manager import BaseDatasetConfiguration, Dataset, get_config


class CRU_TS_Configuration(BaseDatasetConfiguration):
    start_year: int
    end_year: int
    cru_version: str
    cru_url_dir: str
    raw_dir: str
    output_dir: str
    overwrite_download: bool
    overwrite_unzip: bool
    overwrite_processing: bool


class CRU_TS(Dataset):

    name = "Climatic Research Unit gridded Time Series"

    def __init__(self, config: CRU_TS_Configuration):

        self.cru_version = config.cru_version
        self.cru_url_dir = config.cru_url_dir
        self.dl_file_years_str = "1901.2022"

        # note that later in the download URL, there is no second underscore
        # there is more code in self.download() to correct for this difference
        self.cru_label = f"cru_ts_{self.cru_version}"

        self.raw_dir = Path(config.raw_dir) / self.cru_label
        self.output_dir = Path(config.output_dir) / self.cru_label

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.overwrite_download = config.overwrite_download
        self.overwrite_unzip = config.overwrite_unzip
        self.overwrite_process = config.overwrite_processing

        self.years = range(int(config.start_year), int(config.end_year) + 1)
        self.months = range(1, 13)

        temporal_list = [
            "{}{}".format(y, str(m).zfill(2)) for y in self.years for m in self.months
        ]
        band_list = range(1, len(temporal_list) + 1)
        self.band_temporal_list = list(zip(band_list, temporal_list))

        self.var_list = [
            # "cld",
            # "dtr",
            # "frs",
            # "pet",
            "pre",
            # "tmn",
            "tmp",
            # "tmx",
            # "vap",
            # "wet"
        ]

        self.method_list = ["mean", "min", "max", "sum"]

    def download(self, var):
        logger = self.get_logger()

        # these need to be edited for future versions!
        base_url = f"https://crudata.uea.ac.uk/cru/data/hrg/{self.cru_label}/{self.cru_url_dir}/"

        file_prefix = f"cru_ts{self.cru_version}.{self.dl_file_years_str}"

        # determine final destinations of files
        download_filename = f"{file_prefix}.{var}.dat.nc.gz"
        final_dl_path = self.raw_dir / download_filename
        final_gzip_path = self.raw_dir / f"{file_prefix}.{var}.dat.nc"

        if final_dl_path.exists() and not self.overwrite_download:
            logger.info(f"Skipping {str(final_dl_path)}, already downloaded")
        else:
            with self.tmp_to_dst_file(final_dl_path) as dl_dst:
                dl_src = parse.urljoin(base_url, f"{var}/{download_filename}")
                logger.debug(f"Attempting to download: {str(dl_src)}")
                try:
                    request.urlretrieve(dl_src, dl_dst)
                except:
                    logger.exception(f"Failed to download: {str(dl_src)}")
                else:
                    logger.info(f"Successfully downloaded {str(dl_src)}")

        if final_gzip_path.exists() and not self.overwrite_unzip:
            logger.info(f"Skipping {str(final_gzip_path)}, already unzipped")
        else:
            with self.tmp_to_dst_file(final_gzip_path) as gzip_dst:
                logger.debug(
                    f"Attempting to unzip {str(final_dl_path)} to {str(final_gzip_path)}"
                )
                try:
                    with gzip.open(final_dl_path.as_posix(), "rb") as src, open(
                        gzip_dst, "wb"
                    ) as dst:
                        dst.write(src.read())
                except:
                    logger.exception(
                        f"Failed to unzip {str(final_dl_path)} to {str(final_gzip_path)}"
                    )
                else:
                    logger.info(f"Successfully unzipped {str(final_gzip_path)}")

    def extract_layer(self, input, out_path, band):
        """convert specified netcdf band to geotiff and output"""
        if isinstance(input, str):
            src = rasterio.open(input)
        elif isinstance(input, rasterio.io.DatasetReader):
            src = input
        else:
            raise ValueError(f"Invalid input type {type(input)}")
        data = src.read(band)
        meta = {
            "count": 1,
            "crs": {"init": "epsg:4326"},
            "dtype": src.meta["dtype"],
            "transform": src.meta["transform"],
            "driver": "COG",
            "height": src.meta["height"],
            "width": src.meta["width"],
            "nodata": src.meta["nodata"],
            "compress": "lzw",
        }
        with self.tmp_to_dst_file(out_path) as dst_path:
            with rasterio.open(dst_path, "w", **meta) as dst:
                dst.write(np.array([data]))

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
        logger = self.get_logger()
        store = None
        for ix, file_path in enumerate(file_list):
            try:
                raster = rasterio.open(file_path)
            except:
                logger.error(f"Could not include file in aggregation ({file_path})")
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

    def run_yearly_data(self, year, method, var):
        logger = self.get_logger()
        logger.info(f"Running: {var}, {method}, {str(year)}")
        src_base = self.output_dir / "monthly" / var
        dst_base = self.output_dir / "yearly" / var / method
        year_files = sorted(
            [i for i in src_base.iterdir() if f"cru.{var}.{year}" in i.name]
        )
        year_mask = f"cru.{var}.YYYY.tif"
        year_path = dst_base / year_mask.replace("YYYY", str(year))
        # aggregate
        data, meta = self.aggregate_rasters(year_files, method)
        # write geotiff
        meta["dtype"] = data.dtype
        meta["driver"] = "COG"
        meta["compress"] = "lzw"
        with rasterio.open(year_path, "w", **meta) as result:
            result.write(data)

    def run_monthly_data(self, var):
        logger = self.get_logger()

        logger.info(f"Running variable: {var}")
        var_dir = self.output_dir / "monthly" / var
        var_dir.mkdir(parents=True, exist_ok=True)
        in_path = f"netcdf:{self.raw_dir.as_posix()}/cru_ts{self.cru_version}.1901.2022.{var}.dat.nc:{var}"

        src = rasterio.open(in_path)

        for band, temporal in self.band_temporal_list:
            logger.debug(f"processing band {repr(band)}, temporal {repr(temporal)}")
            fname = f"cru.{var}.{temporal}.tif"
            out_path = var_dir / fname
            self.extract_layer(src, out_path, band)

        src.close()

    def main(self):
        logger = self.get_logger()

        var_tasks = [[i] for i in self.var_list]

        logger.info("Downloading and Extracting")
        dl_results = self.run_tasks(self.download, var_tasks)
        self.log_run(dl_results)

        logger.info("Processing Monthly Data")
        monthly_results = self.run_tasks(self.run_monthly_data, var_tasks)
        self.log_run(monthly_results)

        logger.info("Processing Yearly Data")

        qlist = []
        for var in self.var_list:
            for method in self.method_list:
                dst_base = self.output_dir / "yearly" / var / method
                dst_base.mkdir(parents=True, exist_ok=True)
                for year in self.years:
                    qlist.append([year, method, var])

        yearly_results = self.run_tasks(self.run_yearly_data, qlist)
        self.log_run(yearly_results)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def cru_ts(config: CRU_TS_Configuration):
        CRU_TS(config).run(config.run)


if __name__ == "__main__":
    config = get_config(CRU_TS_Configuration)
    CRU_TS(config).run(config.run)
