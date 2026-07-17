"""
DMSP-OLS Nighttime Lights (Version 4 composites)

Downloads Version 4 DMSP-OLS data from the Earth Observation Group (EOG) at
Colorado School of Mines and produces the Elvidge-2014 inter-satellite
calibrated stable-lights composites.

Two source sections are downloaded (see the README):
  - v4 composites (stable_lights.avg_vis) -> our Elvidge-2014 calibration ->
    the final COG product
  - avg_lights_x_pct (.tgz archives)      -> downloaded/extracted only

EOG moved programmatic access behind a paid OAuth tier, so downloads
authenticate with a browser mod_auth_openidc session cookie (same pattern as
viirs_ntl / dvnl): a background thread pings a protected URL every
KEEPALIVE_INTERVAL seconds to keep the short-lived session warm. Grab a fresh
cookie right before running.
"""

import os
import tarfile
import threading
from pathlib import Path

import numpy as np
import rasterio
import requests
from bs4 import BeautifulSoup
from data_manager import BaseDatasetConfiguration, Dataset, get_config

from intercalibration_coefficients import COEFFICIENTS

KEEPALIVE_URL = "https://eogdata.mines.edu/wwwdata/dmsp/v4composites_rearrange/"
KEEPALIVE_INTERVAL = 30  # seconds

COMPOSITES_BASE = "https://eogdata.mines.edu/wwwdata/dmsp/v4composites_rearrange"
AVG_X_PCT_BASE = "https://eogdata.mines.edu/wwwdata/dmsp/v4avg_lights_x_pct"

# DMSP v4 is a completed historical archive (no new data), so the satellite-year
# selection is fixed rather than configurable: one satellite per year, matching
# the years for which Elvidge-2014 calibration coefficients exist.
COMPOSITE_SATYEARS = [
    "F101992", "F101993",
    "F121994", "F121995", "F121996",
    "F141997", "F141998", "F141999",
    "F152000", "F152001", "F152002", "F152003",
    "F162004", "F162005", "F162006", "F162007", "F162008", "F162009",
    "F182010", "F182011", "F182012", "F182013",
]

# avg_lights_x_pct archives. The version suffix is predictable here (unlike the
# composites, whose suffix varies v4b/v4c/v4d and is discovered by listing).
# F182013 is a stable-lights (not avg) raster and has no avg_x_pct archive.
AVG_X_PCT_SATYEARS = {
    "v4b": [
        "F101992", "F101993",
        "F121994", "F121995", "F121996",
        "F141997", "F141998", "F141999",
        "F152000", "F152001", "F152002", "F152003",
        "F162004", "F162005", "F162006", "F162007", "F162008", "F162009",
    ],
    "v4c": ["F182010", "F182011", "F182012"],
}


class DMSPOLSConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    # download the v4 composites and build the calibrated stable-lights product
    run_composites: bool
    # download + extract the avg_lights_x_pct archives (no output product built)
    run_avg_x_pct: bool
    # Browser session cookie (mod_auth_openidc_session) for eogdata.mines.edu.
    # Provided via the gitignored .env / deployment parameter, not committed.
    mod_auth_openidc_session: str
    overwrite_download: bool
    overwrite_processing: bool


class DMSPOLS(Dataset):

    name = "DMSP-OLS Nighttime Lights"

    def __init__(self, config: DMSPOLSConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.run_composites = config.run_composites
        self.run_avg_x_pct = config.run_avg_x_pct
        self.cookies = {"mod_auth_openidc_session": config.mod_auth_openidc_session}
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing
        self.coef = COEFFICIENTS["ELVIDGE2014"]

    def test_connection(self):
        # A protected path redirects (302) to the login when the session cookie
        # is missing/expired; requests follows that to a 200 login page, so
        # disable redirects and treat a redirect as an auth failure. Fail fast.
        r = requests.get(
            KEEPALIVE_URL, cookies=self.cookies, allow_redirects=False, verify=True
        )
        if r.is_redirect:
            raise RuntimeError(
                "eogdata redirected to login: the mod_auth_openidc_session cookie "
                "is missing or expired. Grab a fresh one (see README)."
            )
        r.raise_for_status()

    def start_keepalive(self):
        """
        Background thread pinging a protected URL every KEEPALIVE_INTERVAL
        seconds to keep the EOG session cookie warm. Returns a threading.Event;
        set it to stop pinging.
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

    def _list_links(self, dir_url):
        """GET a protected directory listing and return the href file names."""
        r = requests.get(
            dir_url,
            headers={"User-Agent": "Mozilla/5.0"},
            cookies=self.cookies,
            allow_redirects=False,
        )
        if r.is_redirect:
            raise RuntimeError(
                f"redirected to login listing {dir_url}: EOG session cookie expired."
            )
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        return [str(a.get("href", "")).rsplit("/", 1)[-1] for a in soup.find_all("a")]

    def _download(self, url, dst_path, tmp_dir):
        """Stream a URL to dst_path via a temp file, guarding against a login redirect."""
        with self.tmp_to_dst_file(dst_path, make_dst_dir=True, tmp_dir=tmp_dir) as tmp:
            with requests.get(
                url, cookies=self.cookies, stream=True, allow_redirects=False
            ) as src:
                if src.is_redirect:
                    raise RuntimeError(
                        f"redirected to login (cookie expired?): {url}"
                    )
                src.raise_for_status()
                with open(tmp, "wb") as dst:
                    for chunk in src.iter_content(chunk_size=1024 * 1024):
                        dst.write(chunk)

    # ----- v4 composites: download raw stable lights, then calibrate -----

    def download_composite(self, satyear):
        """
        Download the raw stable-lights composite for one satellite-year. The
        version suffix (v4b/v4c/v4d) varies, so list the directory and pick the
        raw stable_lights.avg_vis.tif (EOG also serves a pre-intercal version,
        which we skip since we apply our own calibration).
        """
        logger = self.get_logger()
        sat, year = satyear[0:3], satyear[3:7]
        dir_url = f"{COMPOSITES_BASE}/{sat}_{year}/"
        local_filename = self.raw_dir / "v4composites" / f"{satyear}_stable_lights.avg_vis.tif"

        if local_filename.exists() and not self.overwrite_download:
            logger.info(f"Download exists: {local_filename}")
            return (satyear, local_filename)

        file_link = None
        for name in self._list_links(dir_url):
            if name.endswith("stable_lights.avg_vis.tif") and "intercal" not in name:
                file_link = name
                break
        if file_link is None:
            raise RuntimeError(f"No raw stable_lights.avg_vis.tif found in {dir_url}")

        self._download(dir_url + file_link, local_filename, self.raw_dir)
        logger.info(f"Downloaded {local_filename}")
        return (satyear, local_filename)

    def calibrate(self, satyear, raw_path):
        """
        Apply the Elvidge-2014 inter-satellite calibration to a raw stable-lights
        composite and write a Cloud Optimized GeoTIFF.

        dn_adjusted = c0 + c1*dn + c2*dn^2, capped at 63. Background (0) stays 0
        and the 255 nodata value is preserved.
        """
        logger = self.get_logger()
        sat, year = satyear[0:3], satyear[3:7]
        output_path = self.output_dir / f"dmsp_ols_{satyear}_stable_lights_calibrated.tif"

        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Calibrated file exists: {output_path}")
            return (satyear, output_path)

        c0, c1, c2 = self.coef[sat][year][:3]

        with rasterio.open(raw_path) as src:
            profile = src.profile.copy()
            data = src.read(1)

        masked = np.ma.MaskedArray(data, mask=data == 0)
        out = c0 + (c1 * masked) + (c2 * masked ** 2)
        out[np.where(out > 63)] = 63
        out = np.uint8(np.round(out))
        out[np.where(masked == 255)] = 255
        out = out.filled(0)

        profile.update(
            driver="COG", compress="LZW", dtype="uint8", count=1, nodata=255
        )
        # COG driver rejects these GTiff-only creation options if carried over
        for k in ("blockxsize", "blockysize", "tiled", "interleave"):
            profile.pop(k, None)

        with self.tmp_to_dst_file(
            output_path, make_dst_dir=True, tmp_dir=self.output_dir, validate_cog=True
        ) as tmp_dst:
            with rasterio.open(tmp_dst, "w", **profile) as dst:
                dst.write(out, 1)

        logger.info(f"Calibrated {output_path}")
        return (satyear, output_path)

    # ----- avg_lights_x_pct: download + extract only -----

    def download_avg_x_pct(self, satyear, version):
        """
        Download and extract one avg_lights_x_pct archive. Directory listing is
        forbidden, but direct file access works and the version suffix is
        predictable, so the URL is constructed rather than discovered.
        """
        logger = self.get_logger()
        filename = f"{satyear}.{version}.avg_lights_x_pct.tgz"
        archive_path = self.raw_dir / "v4avg_lights_x_pct" / filename
        extract_dir = self.raw_dir / "v4avg_lights_x_pct" / satyear

        if extract_dir.exists() and not self.overwrite_download:
            logger.info(f"Extracted avg_x_pct exists: {extract_dir}")
            return (satyear, extract_dir)

        self._download(f"{AVG_X_PCT_BASE}/{filename}", archive_path, self.raw_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive_path) as tar:
            tar.extractall(extract_dir, filter="data")

        logger.info(f"Downloaded and extracted {filename}")
        return (satyear, extract_dir)

    def main(self):
        logger = self.get_logger()

        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        # Keep the EOG session cookie warm for every step that hits eogdata.
        stop_keepalive = self.start_keepalive()
        try:
            logger.info("Testing connection...")
            self.test_connection()

            if self.run_composites:
                logger.info("Downloading v4 composites (raw stable lights)")
                dl = self.run_tasks(
                    self.download_composite, [(sy,) for sy in COMPOSITE_SATYEARS]
                )
                self.log_run(dl)

                logger.info("Calibrating composites (Elvidge 2014)")
                cal = self.run_tasks(
                    self.calibrate, [r.args for r in dl.results() if r]
                )
                self.log_run(cal)

            if self.run_avg_x_pct:
                logger.info("Downloading + extracting avg_lights_x_pct")
                avg_items = [
                    (sy, ver)
                    for ver, sats in AVG_X_PCT_SATYEARS.items()
                    for sy in sats
                ]
                avg = self.run_tasks(self.download_avg_x_pct, avg_items)
                self.log_run(avg)
        finally:
            stop_keepalive.set()


try:
    from prefect import flow
except ImportError:
    pass
else:

    @flow
    def dmsp_ols(config: DMSPOLSConfiguration):
        DMSPOLS(config).run(config.run)


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()
    config = get_config(DMSPOLSConfiguration)
    # secret comes from the gitignored .env for local runs
    config.mod_auth_openidc_session = os.environ.get("mod_auth_openidc_session")
    DMSPOLS(config).run(config.run)
