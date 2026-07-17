# data download script for DVNL ntl data
# info link: https://eogdata.mines.edu/products/dmsp/#dvnl
import os
import threading
from copy import copy
from pathlib import Path

import rasterio
import requests
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from rasterio import windows

# EOG (eogdata.mines.edu) moved programmatic access behind a paid OAuth tier, so
# downloads now authenticate with a browser session cookie (mod_auth_openidc)
# instead. The session has a short inactivity timeout, so a background thread
# pings a protected URL on this interval to keep it warm for the duration of a
# run. Grab a fresh cookie right before running — see the README.
KEEPALIVE_URL = "https://eogdata.mines.edu/wwwdata/viirs_products/dvnl/"
KEEPALIVE_INTERVAL = 30  # seconds


class DVNLConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    # Comma-separated years (e.g. "2000,2001"). String, not list, so the
    # Prefect run form renders a text input rather than the array widget,
    # whose "add item" button submits the form.
    years: str
    # Browser session cookie (mod_auth_openidc_session) for eogdata.mines.edu.
    # Provided via the gitignored .env / deployment parameter, not committed.
    mod_auth_openidc_session: str
    overwrite_download: bool
    overwrite_processing: bool


class DVNL(Dataset):

    name = "DVNL"

    def __init__(self, config: DVNLConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = [int(v.strip()) for v in config.years.split(",") if v.strip()]
        self.cookies = {"mod_auth_openidc_session": config.mod_auth_openidc_session}
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing
        self.download_url = (
            "https://eogdata.mines.edu/wwwdata/viirs_products/dvnl/DVNL_{YEAR}.tif"
        )

    def test_connection(self):
        # A protected path redirects (302) to the login when the session cookie
        # is missing/expired; requests follows that to a 200 login page, so
        # disable redirects and treat a redirect as an auth failure. This is the
        # earliest signal that the cookie is bad, so fail fast here.
        test_request = requests.get(
            "https://eogdata.mines.edu/wwwdata/viirs_products/dvnl/",
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

    def manage_download(self, year):
        """
        Download individual file
        """

        logger = self.get_logger()

        download_dest = self.download_url.format(YEAR=year)
        local_filename = self.raw_dir / f"raw_dvnl_{year}.tif"

        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {download_dest}")
        else:
            with requests.get(
                download_dest,
                cookies=self.cookies,
                stream=True,
                allow_redirects=False,
                verify=True,
            ) as r:
                # A redirect here is the login page: the session cookie is
                # missing/expired. Fail the task loudly rather than writing an
                # HTML login page to a .tif.
                if r.is_redirect:
                    raise RuntimeError(
                        f"redirected to login (EOG session cookie expired?): {download_dest}"
                    )
                r.raise_for_status()
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            logger.info(f"Downloaded: {download_dest}")

        return (download_dest, local_filename)

    def convert_to_cog(self, year):
        """
        Convert GeoTIFF to Cloud Optimized GeoTIFF (COG)
        """
        logger = self.get_logger()

        src_path = self.raw_dir / f"raw_dvnl_{year}.tif"
        dst_path = self.output_dir / f"dvnl_{year}.tif"

        if os.path.isfile(dst_path) and not self.overwrite_processing:
            logger.info(f"Converted File Exists: {dst_path}")
            return (src_path, dst_path)

        else:
            with rasterio.open(src_path, "r") as src:

                profile = copy(src.profile)

                profile.update(
                    {
                        "driver": "COG",
                        "compress": "LZW",
                    }
                )

                with rasterio.open(dst_path, "w+", **profile) as dst:

                    for ji, src_window in src.block_windows(1):
                        # convert relative input window location to relative output window location
                        # using real world coordinates (bounds)
                        src_bounds = windows.bounds(
                            src_window, transform=src.profile["transform"]
                        )
                        dst_window = windows.from_bounds(
                            *src_bounds, transform=dst.profile["transform"]
                        )
                        # round the values of dest_window as they can be float
                        dst_window = windows.Window(
                            round(dst_window.col_off),
                            round(dst_window.row_off),
                            round(dst_window.width),
                            round(dst_window.height),
                        )
                        # read data from source window
                        r = src.read(1, window=src_window)
                        # write data to output window
                        dst.write(r, 1, window=dst_window)
            logger.info(f"File Converted: {dst_path}")
            return (src_path, dst_path)

    def main(self):

        logger = self.get_logger()

        os.makedirs(self.raw_dir, exist_ok=True)

        # Keep the EOG session cookie warm for every step that touches
        # eogdata.mines.edu (connection test and downloads).
        stop_keepalive = self.start_keepalive()
        try:
            logger.info("Testing Connection...")
            self.test_connection()

            logger.info("Running data download")
            download = self.run_tasks(self.manage_download, [[y] for y in self.years])
            self.log_run(download)
        finally:
            stop_keepalive.set()

        os.makedirs(self.output_dir, exist_ok=True)

        logger.info("Converting raw tifs to COGs")
        conversions = self.run_tasks(self.convert_to_cog, [[y] for y in self.years])
        self.log_run(conversions)


try:
    from prefect import flow
except:
    pass
else:

    @flow
    def dvnl(config: DVNLConfiguration):
        DVNL(config).run(config.run)


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()
    config = get_config(DVNLConfiguration)
    # secret comes from the gitignored .env for local runs
    config.mod_auth_openidc_session = os.environ.get("mod_auth_openidc_session")
    DVNL(config).run(config.run)
