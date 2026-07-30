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

# monthly data isn't configurable by version (unlike annual, via
# annual_version) — the download URL is hardcoded to v10
MONTHLY_VERSION = "v10"


class VIIRS_NTL_Configuration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    run_annual: bool
    # Comma-separated (e.g. "a,b"). String, not list, so the Prefect run
    # form renders a text input rather than the array widget, whose "add
    # item" button submits the form.
    annual_file_types: str
    run_monthly: bool
    # Comma-separated (e.g. "a,b"). String, not list, so the Prefect run
    # form renders a text input rather than the array widget, whose "add
    # item" button submits the form.
    monthly_file_types: str
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
        self.annual_file_types = [v.strip() for v in config.annual_file_types.split(",") if v.strip()]
        self.run_monthly: bool = config.run_monthly
        self.monthly_file_types = [v.strip() for v in config.monthly_file_types.split(",") if v.strip()]
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

    def get_annual_version_info(self, year) -> tuple[str, str]:
        if int(year) < 2022:
            annual_version = "v21"
            file_config = "vcmcfg" if int(year) < 2014 else "vcmslcfg"
        else:
            annual_version = "v22"
            file_config = "vcmslcfg"
        return annual_version, file_config

    def build_download_list(self):
        task_list = []
        logger = self.get_logger()

        if self.run_annual:
            for year in self.years:
                annual_version, file_config = self.get_annual_version_info(year)

                # Filenames embed a processing timestamp (e.g. "c202303062300")
                # that changes whenever EOG reprocesses a year, so it can't be
                # hardcoded. Scrape the year's directory listing instead (same
                # approach as the monthly listing below) and match on the
                # parts of the filename that are actually stable.
                dir_url = f"https://eogdata.mines.edu/nighttime_light/annual/{annual_version}/{year}/"
                link_list: List[str] = []
                attempts = 1
                while attempts <= self.max_retries:
                    try:
                        r = requests.get(
                            dir_url,
                            headers={"User-Agent": "Mozilla/5.0"},
                            cookies=self.cookies,
                            allow_redirects=False,
                        )
                        if r.is_redirect:
                            raise RuntimeError(
                                f"Redirected to login listing {dir_url}: EOG session cookie expired."
                            )
                        soup = BeautifulSoup(r.content, "html.parser")
                        for i in soup.find_all("tr"):
                            link = str(i.findChild("a")["href"])
                            link_list.append(urllib.parse.urljoin(dir_url, link))
                        break
                    except Exception as e:
                        attempts += 1
                        if attempts > self.max_retries:
                            logger.info(f"Failed to list {dir_url}: {str(e)}")
                        else:
                            logger.info("Retrying listing: " + str(dir_url))

                # IMPORTANT: 2012's composite window is ambiguous (it can be
                # either Apr-Dec 2012 or Apr 2012-Mar 2013) - match the range
                # we intend rather than just the bare year.
                year_fragment = "201204-201303" if int(year) == 2012 else str(year)
                config_fragment = f"_global_{file_config}_"

                for ftype in self.annual_file_types:
                    download_dest: Optional[str] = None
                    for link in link_list:
                        if (
                            year_fragment in link
                            and config_fragment in link
                            and link.endswith(f"{ftype}.dat.tif.gz")
                        ):
                            download_dest = link
                            break
                    if download_dest is None:
                        logger.info(
                            f"Download option does not exist yet: {year}/{ftype}"
                        )
                        continue

                    local_filename = (
                        self.raw_dir / "annual" / f"raw_viirs_ntl_{annual_version}_{year}_{ftype}.tif.gz"
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

                    for ftype in self.monthly_file_types:
                        file_link: Optional[str] = None
                        for link in link_list:
                            if link.endswith(f"{ftype}.tif.gz"):
                                file_link = link
                                break
                        if file_link is None:
                            logger.info(
                                f"Download option does not exist yet: {str(year)}/{format_month}/{ftype}"
                            )
                        else:
                            local_filename = (
                                self.raw_dir
                                / "monthly" / f"raw_viirs_ntl_{year}_{format_month}_{ftype}.tif.gz"
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
            local_filename.parent.mkdir(parents=True, exist_ok=True)
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
                annual_version, file_config = self.get_annual_version_info(year)
                for file in self.annual_file_types:
                    raw_local_filename = (
                        self.raw_dir / "annual" /f"raw_viirs_ntl_{annual_version}_{year}_{file}.tif.gz"
                    )
                    output_filename = (
                        self.raw_dir / "annual" /f"raw_extracted_viirs_ntl_{year}_{file}.tif"
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
                    for file in self.monthly_file_types:
                        format_month = str(month).zfill(2)

                        raw_local_filename = (
                            self.raw_dir / "monthly"
                            / f"raw_viirs_ntl_{year}_{format_month}_{file}.tif.gz"
                        )
                        output_filename = (
                            self.raw_dir / "monthly"
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
            annual_dir = self.output_dir / "annual"
            for year in self.years:
                for ftype in self.annual_file_types:
                    raw_annual_glob_str = (
                        self.raw_dir / "annual" / f"raw_extracted_viirs_ntl_{year}_{ftype}.tif"
                    )
                    output_annual_glob = (
                        annual_dir / f"viirs_ntl_annual_{year}_{ftype}.tif"
                    )
                    if raw_annual_glob_str.exists():
                        task_list.append((raw_annual_glob_str, output_annual_glob))
                    else:
                        logger.info(
                            f"Failed to find extracted raw file: {str(raw_annual_glob_str)}"
                        )

        if self.run_monthly:
            monthly_dir = self.output_dir / "monthly" / MONTHLY_VERSION
            for year in self.years:
                for month in self.months:
                    format_month = str(month).zfill(2)
                    for ftype in self.monthly_file_types:
                        raw_monthly_glob_str = (
                            self.raw_dir / "monthly"
                            / f"raw_extracted_viirs_ntl_{year}_{format_month}_{ftype}.tif"
                        )
                        output_monthly_glob = (
                            monthly_dir
                            / f"viirs_ntl_monthly_{year}_{format_month}_{ftype}.tif"
                        )
                        if raw_monthly_glob_str.exists():
                            task_list.append((raw_monthly_glob_str, output_monthly_glob))
                        else:
                            logger.info(
                                f"Failed to find extracted raw file: {str(raw_monthly_glob_str)}"
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
                    output_path, make_dst_dir=True, validate_cog=True
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
