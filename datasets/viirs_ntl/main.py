# data download for viirs nighttime lights data
# data from: https://eogdata.mines.edu/nighttime_light/

import gzip
import os
import shutil
import threading
import urllib.parse
from pathlib import Path
from typing import List, Optional

import numpy as np
import rasterio
import requests
from bs4 import BeautifulSoup
from data_manager import BaseDatasetConfiguration, Dataset, get_config

# EOG (eogdata.mines.edu) moved programmatic access behind a paid OAuth tier, so
# downloads now authenticate with a browser session cookie (mod_auth_openidc)
# instead. The session has a short inactivity timeout, so a background thread
# pings a protected URL on this interval to keep it warm for the duration of a
# run. Grab a fresh cookie right before running — see the README.
KEEPALIVE_URL = "https://eogdata.mines.edu/nighttime_light/"
KEEPALIVE_INTERVAL = 30  # seconds


class VIIRS_NTL_Configuration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    run_annual: bool
    # Comma-separated (e.g. "a,b"). String, not list, so the Prefect run
    # form renders a text input rather than the array widget, whose "add
    # item" button submits the form.
    annual_files: str
    annual_version: str
    run_monthly: bool
    # Comma-separated (e.g. "a,b"). String, not list, so the Prefect run
    # form renders a text input rather than the array widget, whose "add
    # item" button submits the form.
    monthly_files: str
    # Comma-separated (e.g. "a,b"). String, not list, so the Prefect run
    # form renders a text input rather than the array widget, whose "add
    # item" button submits the form.
    months: str
    # Comma-separated (e.g. "a,b"). String, not list, so the Prefect run
    # form renders a text input rather than the array widget, whose "add
    # item" button submits the form.
    years: str
    # Browser session cookie (mod_auth_openidc_session) for eogdata.mines.edu.
    # Provided via the gitignored .env / deployment parameter, not committed.
    mod_auth_openidc_session: str
    max_retries: int
    cf_minimum: int
    overwrite_download: bool
    overwrite_extract: bool
    overwrite_processing: bool


class VIIRS_NTL(Dataset):
    name = "VIIRS Nighttime Lights"

    def __init__(self, config: VIIRS_NTL_Configuration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.run_annual: bool = config.run_annual
        self.annual_version: str = config.annual_version
        self.annual_files = [v.strip() for v in config.annual_files.split(",") if v.strip()]
        self.run_monthly: bool = config.run_monthly
        self.monthly_files = [v.strip() for v in config.monthly_files.split(",") if v.strip()]
        self.months = [int(v.strip()) for v in config.months.split(",") if v.strip()]
        self.years = [int(v.strip()) for v in config.years.split(",") if v.strip()]
        self.cookies = {"mod_auth_openidc_session": config.mod_auth_openidc_session}
        self.max_retries: int = config.max_retries
        self.cf_minimum = config.cf_minimum
        self.overwrite_download: bool = config.overwrite_download
        self.overwrite_extract: bool = config.overwrite_extract
        self.overwrite_processing: bool = config.overwrite_processing

    def test_connection(self):
        # A protected path redirects (302) to the login when the session cookie
        # is missing/expired; requests follows that to a 200 login page, so
        # disable redirects and treat a redirect as an auth failure. This is the
        # earliest signal that the cookie is bad, so fail fast here.
        test_request = requests.get(
            "https://eogdata.mines.edu/nighttime_light/",
            cookies=self.cookies,
            allow_redirects=False,
            verify=True,
        )
        if test_request.is_redirect:
            raise RuntimeError(
                "eogdata redirected to login: the mod_auth_openidc_session cookie "
                "is missing or expired. Grab a fresh one (see README)."
            )
        test_request.raise_for_status()

    def start_keepalive(self):
        """
        Start a background thread that pings a protected URL every
        KEEPALIVE_INTERVAL seconds to keep the EOG session cookie warm for the
        duration of a run. Returns a threading.Event; set it to stop pinging.
        """
        logger = self.get_logger()
        stop = threading.Event()

        def ping():
            while not stop.wait(KEEPALIVE_INTERVAL):
                try:
                    r = requests.get(
                        KEEPALIVE_URL,
                        cookies=self.cookies,
                        allow_redirects=False,
                        timeout=30,
                    )
                    if r.is_redirect:
                        logger.warning(
                            "Keep-alive ping was redirected to login: the EOG "
                            "session cookie appears to have expired mid-run."
                        )
                except Exception as e:
                    logger.warning(f"Keep-alive ping failed: {e}")

        threading.Thread(target=ping, name="eog-keepalive", daemon=True).start()
        return stop

    def build_download_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            # TODO: pull from beautiful soup for file url, filter out non-available urls here
            for year in self.years:
                for file in self.annual_files:
                    if self.annual_version == "v21":
                        if int(year) == 2012:
                            # IMPORTANT: this can either be 201204-201212 or 201204-201303
                            # depending on what we prefer!
                            download_url = "https://eogdata.mines.edu/nighttime_light/annual/v21/{YEAR}/VNL_v21_npp_201204-201303_global_{CONFIG}_c202205302300.{TYPE}.dat.tif.gz"
                        else:
                            download_url = "https://eogdata.mines.edu/nighttime_light/annual/v21/{YEAR}/VNL_v21_npp_{YEAR}_global_{CONFIG}_c202205302300.{TYPE}.dat.tif.gz"
                        if int(year) < 2014:
                            file_config = "vcmcfg"
                        else:
                            file_config = "vcmslcfg"
                    elif self.annual_version == "v22":
                        file_config = "vcmslcfg"
                        if int(year) == 2022:
                            download_url = "https://eogdata.mines.edu/nighttime_light/annual/v22/{YEAR}/VNL_v22_npp-j01_{YEAR}_global_{CONFIG}_c202303062300.{TYPE}.dat.tif.gz"
                        else:
                            download_url = "https://eogdata.mines.edu/nighttime_light/annual/v22/{YEAR}/VNL_npp_{YEAR}_global_{CONFIG}_v2_c202402081600.{TYPE}.dat.tif.gz"
                    else:
                        raise NotImplementedError(
                            f"Annual version {self.annual_version} is not yet supported."
                        )
                    download_dest = download_url.format(
                        YEAR=year, TYPE=file, CONFIG=file_config
                    )
                    local_filename = (
                        self.raw_dir / f"raw_viirs_ntl_{year}_{file}.tif.gz"
                    )
                    task_list.append((download_dest, local_filename))
        if self.run_monthly:
            for year in self.years:
                for month in self.months:
                    format_month = str(month).zfill(2)

                    if year == 2012 and month in [1, 2, 3]:
                        # dataset starts in April 2012!
                        continue

                    if (year == 2022) & (month == 8):
                        download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/NOAA-20/vcmcfg/"
                    else:
                        download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmcfg/"

                    download_url = download_url.format(
                        YEAR=str(year), MONTH=format_month
                    )

                    attempts = 1
                    while attempts <= self.max_retries:
                        try:
                            r = requests.get(
                                download_url,
                                headers={"User-Agent": "Mozilla/5.0"},
                                cookies=self.cookies,
                                allow_redirects=False,
                            )
                            if r.is_redirect:
                                raise RuntimeError(
                                    "Redirected to login listing "
                                    f"{download_url}: EOG session cookie expired."
                                )
                            soup = BeautifulSoup(r.content, "html.parser")

                            items = soup.find_all("tr")
                            link_list: List[str] = []

                            for i in items:
                                link = str(i.findChild("a")["href"])
                                absolute_link = urllib.parse.urljoin(download_url, link)
                                link_list.append(absolute_link)

                            break

                        except Exception as e:
                            attempts += 1
                            if attempts > self.max_retries:
                                logger.info(
                                    f"Failed to download: {str(download_dest)}: {str(e)}"
                                )
                            else:
                                logger.info("Retrieved: " + str(download_dest))

                    for file in self.monthly_files:
                        file_link: Optional[str] = None
                        for link in link_list:
                            if link.endswith(f"{file}.tif.gz"):
                                file_link = link
                                break
                        if file_link is None:
                            logger.info(
                                f"Download option does not exist yet: {str(year)}/{format_month}/{file}"
                            )
                        else:
                            local_filename = (
                                self.raw_dir
                                / f"raw_viirs_ntl_{year}_{format_month}_{file}.tif.gz"
                            )
                            task_list.append((file_link, local_filename))

        return task_list

    def manage_download(self, download_dest, local_filename):
        # consider doing separate directories for years when doing monthly data download
        """
        Download individual file
        """

        logger = self.get_logger()

        if local_filename.exists() and not self.overwrite_download:
            logger.info(f"Download Exists: {local_filename}")
        else:
            logger.info(f"Attempting to download from {download_dest}...")
            try:
                with requests.get(
                    download_dest,
                    cookies=self.cookies,
                    stream=True,
                    allow_redirects=False,
                ) as src:
                    # A redirect here is the login page: the session cookie is
                    # missing/expired. Fail the task loudly rather than writing
                    # an HTML login page to a .tif.gz.
                    if src.is_redirect:
                        raise RuntimeError(
                            "redirected to login (EOG session cookie expired?)"
                        )
                    # raise an exception (fail this task) if HTTP response indicates that an error occured
                    src.raise_for_status()
                    with open(local_filename, "wb") as dst:
                        dst.write(src.content)
            except Exception as e:
                raise RuntimeError(
                    str(e) + f": Failed to download: {str(download_dest)}"
                )
            else:
                logger.info(f"Downloaded {str(local_filename)}")

        return (download_dest, local_filename)

    def build_extract_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            for year in self.years:
                for file in self.annual_files:
                    raw_local_filename = (
                        self.raw_dir / f"raw_viirs_ntl_{year}_{file}.tif.gz"
                    )
                    output_filename = (
                        self.raw_dir / f"raw_extracted_viirs_ntl_{year}_{file}.tif"
                    )
                    if raw_local_filename.exists():
                        task_list.append((raw_local_filename, output_filename))
                    else:
                        raise RuntimeError(
                            f"Raw file not located:  {str(raw_local_filename)}"
                        )
        if self.run_monthly:
            for year in self.years:
                for month in self.months:
                    if year == 2012 and month in [1, 2, 3]:
                        # dataset starts in April 2012!
                        continue
                    for file in self.monthly_files:
                        format_month = str(month).zfill(2)

                        raw_local_filename = (
                            self.raw_dir
                            / f"raw_viirs_ntl_{year}_{format_month}_{file}.tif.gz"
                        )
                        output_filename = (
                            self.raw_dir
                            / f"raw_extracted_viirs_ntl_{year}_{format_month}_{file}.tif"
                        )
                        if raw_local_filename.exists():
                            task_list.append((raw_local_filename, output_filename))
                        else:
                            raise RuntimeError(
                                f"Raw file not located:  {str(raw_local_filename)}"
                            )

        return task_list

    def extract_files(self, raw_local_filename, output_filename):
        """
        Extract individual file
        """
        logger = self.get_logger()

        if output_filename.exists() and not self.overwrite_extract:
            logger.info(f"Extracted File Exists: {output_filename}")
            return (raw_local_filename, output_filename)
        else:
            try:
                with gzip.open(raw_local_filename, "rb") as f_in:
                    with open(output_filename, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                logger.info(f"Extracted file to: {output_filename}")
                return (raw_local_filename, output_filename)
            except Exception as e:
                logger.info(f"Failed to extract: {str(raw_local_filename)}")
                raise Exception(
                    str(e) + ": " f"Failed to extract: {str(raw_local_filename)}"
                )

    def build_process_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            for year in self.years:
                annual_avg_glob_str = (
                    self.raw_dir / f"raw_extracted_viirs_ntl_{year}_average_masked.tif"
                )
                output_avg_glob = (
                    self.output_dir / f"viirs_ntl_annual_{year}_avg_masked.tif"
                )
                if annual_avg_glob_str.exists():
                    task_list.append((annual_avg_glob_str, output_avg_glob))
                else:
                    logger.info(
                        f"Failed to find extracted raw file: {str(annual_avg_glob_str)}"
                    )

                annual_cloud_glob_str = (
                    self.raw_dir / f"raw_extracted_viirs_ntl_{year}_cf_cvg.tif"
                )
                output_cloud_glob = (
                    self.output_dir / f"viirs_ntl_annual_{year}_cf_cvg.tif"
                )
                if annual_cloud_glob_str.exists():
                    task_list.append((annual_cloud_glob_str, output_cloud_glob))
                else:
                    logger.info(
                        f"Failed to find extracted raw file: {str(annual_cloud_glob_str)}"
                    )

        if self.run_monthly:
            for year in self.years:
                for month in self.months:
                    format_month = str(month).zfill(2)

                    monthly_avg_glob_str = (
                        self.raw_dir
                        / f"raw_extracted_viirs_ntl_{year}_{format_month}_avg_rade9h.masked.tif"
                    )
                    output_avg_glob = (
                        self.output_dir
                        / f"viirs_ntl_monthly_{year}_{format_month}_avg_masked.tif"
                    )
                    if monthly_avg_glob_str.exists():
                        task_list.append((monthly_avg_glob_str, output_avg_glob))
                    else:
                        logger.info(
                            f"Failed to find extracted raw file: {str(monthly_avg_glob_str)}"
                        )

                    monthly_cloud_glob_str = (
                        self.raw_dir
                        / f"raw_extracted_viirs_ntl_{year}_{format_month}_cf_cvg.tif"
                    )
                    output_cloud_glob = (
                        self.output_dir
                        / f"viirs_ntl_monthly_{year}_{format_month}_cf_cvg.tif"
                    )
                    if monthly_cloud_glob_str.exists():
                        task_list.append((monthly_cloud_glob_str, output_cloud_glob))
                    else:
                        logger.info(
                            f"Failed to find extracted raw file: {str(monthly_cloud_glob_str)}"
                        )

        return task_list

    def raster_calc(self, input_path, output_path, function, **kwargs):
        """
        Calculate raster values using rasterio based on function provided

        :param input_path: input raster
        :param output_path: path to write output raster to
        :param function: function to apply to input raster values
        :param kwargs: additional meta args used to write output raster
        """
        with rasterio.Env(GDAL_CACHEMAX=100, CHECK_DISK_FREE_SPACE=False):
            # GDAL_CACHEMAX value in MB
            # https://trac.osgeo.org/gdal/wiki/ConfigOptions#GDAL_CACHEMAX
            # See: https://github.com/mapbox/rasterio/issues/1281
            with rasterio.open(input_path) as src:
                assert len(set(src.block_shapes)) == 1
                meta = src.meta.copy()
                meta.update(
                    {
                        "driver": "COG",
                        "compress": "LZW",
                    }
                )
                meta.update(**kwargs)
                with self.tmp_to_dst_file(
                    output_path, validate_cog=True
                ) as tmp_dst_path:
                    with rasterio.open(tmp_dst_path, "w", **meta) as dst:
                        for ji, window in src.block_windows(1):
                            in_data = src.read(window=window)
                            out_data = function(in_data)
                            out_data = out_data.astype(meta["dtype"])
                            dst.write(out_data, window=window)

    def remove_negative(self, x):
        """
        remove negative values from array
        """
        return np.where(x > 0, x, 0)

    def make_binary(self, x):
        """
        create binary array based on threshold value
        """
        threshold = self.cf_minimum
        return np.where(x >= threshold, 1, 0)

    def process_files(self, raw_file, output_dst):
        logger = self.get_logger()
        if output_dst.exists() and not self.overwrite_processing:
            logger.info(f"Processed File Exists: {str(raw_file)}")
            return (raw_file, output_dst)
        try:
            if "cf_cvg" in str(raw_file):
                self.raster_calc(raw_file, output_dst, self.make_binary)
            else:
                self.raster_calc(raw_file, output_dst, self.remove_negative)
            logger.info(f"File Processed: {str(output_dst)}")
            return (raw_file, output_dst)
        except Exception as e:
            logger.info(f"Failed to process: {str(raw_file)}")
            raise Exception(str(e) + f": Failed to process: {str(raw_file)}")

    def main(self):
        logger = self.get_logger()

        # Keep the EOG session cookie warm for every step that touches
        # eogdata.mines.edu (connection test, directory listing, downloads).
        stop_keepalive = self.start_keepalive()
        try:
            logger.info("Testing Connection...")
            self.test_connection()

            os.makedirs(self.raw_dir, exist_ok=True)
            logger.info("Building download list...")
            dl_list = self.build_download_list()

            logger.info("Running data download")
            download = self.run_tasks(self.manage_download, dl_list)
            self.log_run(download)
        finally:
            stop_keepalive.set()

        logger.info("Building extract list...")
        extract_list = self.build_extract_list()
        os.makedirs(self.output_dir, exist_ok=True)

        logger.info("Extracting raw files")
        extraction = self.run_tasks(self.extract_files, extract_list)
        self.log_run(extraction)

        logger.info("Building processing list...")
        process_list = self.build_process_list()

        logger.info("Processing raw files")
        process = self.run_tasks(self.process_files, process_list)
        self.log_run(process)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def viirs_ntl(config: VIIRS_NTL_Configuration):
        VIIRS_NTL(config).run(config.run)


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()
    config = get_config(VIIRS_NTL_Configuration)
    # secret comes from the gitignored .env for local runs
    config.mod_auth_openidc_session = os.environ.get("mod_auth_openidc_session")
    VIIRS_NTL(config).run(config.run)
