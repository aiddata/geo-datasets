"""

1. Go to https://landscan.ornl.gov and register for account if you do not have one already (may take several days for them to approve)
2. Then go to https://landscan.ornl.gov/landscan-datasets and open the Developer Tools for your browser (e.g., press F12 in Chrome)
3. Select the "Application" tab in Developer Tools
4. In the "Storage" section of the left hand side menu click "Cookies" and then select "https://landscan.ornl.gov"
5. In the main area you should now see a table of cookies with a "Name" and "Value" column.
6. Copy the "Name" that starts with "SESS" and replace the "cookie_name" variable value in the config
7. Copy the "Value" corresponding to the above Name and replace the "cookie_value" variable value in the config
8. This cookie has an expiration date (<1 month) so you will need to retrieve a new cookie at some point in the future
9. Enter your username and password into the variables in the config

10. Edit the years in the config if needed
11. Set the raw and output data directories in the config

Note: Do not share your username, password, or cookie combination.


"""

import os
import sys
import time
import zipfile
import requests
import glob
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser

import rasterio


sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset


class LandScanPop(Dataset):
    name = "LandScan Population"

    def __init__(self, raw_dir, output_dir, years, run_download=True, run_extract=True, run_conversion=True, cookie_name=None, cookie_value=None, username=None, password=None, overwrite_download=False, overwrite_extract=False, overwrite_conversion=False):

        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)

        self.years = years

        self.run_download = run_download
        self.run_extract = run_extract
        self.run_conversion = run_conversion

        self.cookie_name = cookie_name
        self.cookie_value = cookie_value

        self.username = username
        self.password = password

        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract
        self.overwrite_conversion = overwrite_conversion


        self.download_dir = raw_dir / "compressed"
        self.extract_dir = raw_dir / "uncompressed"

        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)


    def test_connection(self):
        logger = self.get_logger()
        logger.info(f'Testing download connection...')

        session = requests.Session()
        session.auth = (self.username, self.password)
        session.cookies.set(self.cookie_name, self.cookie_value)

        test_request = session.get("https://landscan.ornl.gov/landscan-datasets")
        test_request.raise_for_status()



    def download_file(self, year, local_filename):
        logger = self.get_logger()


        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"File exists, skipping download {local_filename}")
        else:
            logger.info(f"Downloading {year} - {local_filename}")

            # init session with authentication and cookie for each worker
            # TODO: remove this from task func and just run once per worker?
            session = requests.Session()
            session.auth = (self.username, self.password)
            session.cookies.set(self.cookie_name, self.cookie_value)

            base_url = "https://landscan.ornl.gov/system/files"

            # some source files have nonstandard names (e.g. "landscan_2000_0.zip" instead of "landscan_2000.zip")
            # these variables and the while loop support iterating through potential variations to find correct file
            extra_str = ""
            extra_int = 0
            while True:
                dl_link = f"{base_url}/LandScan%20Global%20{year}{extra_str}.zip"
                # download server seems prone to request errors, this allows multiple attempts
                attempts = 0
                response = None
                while attempts < 5:
                    try:
                        response = session.get(dl_link, stream=True)
                        break
                    except:
                        attempts += 1
                        time.sleep(5)

                try:
                    status_code = response.status_code
                except:
                    status_code = None

                if status_code and status_code == 200:
                    with open(local_filename, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=1024*1024):
                            f.write(chunk)
                    response.close()
                    return "Downloaded"
                else:
                    # indicates the current file we are attempting is wrong
                    if status_code:
                        response.close()
                    if extra_int >= 3:
                        raise Exception(f"Could not find valid download link for year {year}")
                    extra_str = f"_{extra_int}"
                    extra_int += 1


    def unzip_file(self, zip_file, out_dir):
        """Extract a zipfile"""
        logger = self.get_logger()
        logger.info(f"Extracting {zip_file} to {out_dir}")
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(out_dir)


    def convert_esri_grid_to_geotiff(self, esri_grid_path, cog_path):
        """Convert a raster from ESRI grid format to COG format"""
        logger = self.get_logger()

        if os.path.isfile(cog_path) and not self.overwrite_conversion:
            logger.info(f"COG exists - skipping ({cog_path})")
        else:
            logger.info(f"Converting to COG ({cog_path})")
            with rasterio.open(esri_grid_path) as src:
                assert len(set(src.block_shapes)) == 1
                meta = src.meta.copy()
                meta.update(driver="COG", compress="LZW")
                with rasterio.open(cog_path, "w", **meta) as dst:
                    for ji, window in src.block_windows(1):
                        in_data = src.read(window=window)
                        dst.write(in_data, window=window)


    def main(self):
        logger = self.get_logger()

        logger.info('Starting pipeline...')

        # download
        if self.run_download:
            logger.info('Testing download connection...')

            self.test_connection()

            logger.info('Running download tasks...')
            dl_list = [(year, self.download_dir / f"LandScan_Global_{year}.zip") for year in self.years]
            dl = self.run_tasks(self.download_file, dl_list)
            self.log_run(dl)


        # unzip
        if self.run_extract:
            logger.info('Running extract tasks...')
            ex_list = [ ( self.download_dir / x, self.extract_dir / x[:-4] ) for x in self.download_dir.iterdir() ]
            extract = self.run_tasks(self.unzip_file, ex_list)
            self.log_run(extract)


        # convert from esri grid format to COG
        if self.run_conversion:
            logger.info('Running conversion tasks...')
            conv_list = [ ( x, self.data_dir / os.path.basename(x)+'.tif' ) for x in glob.glob(self.extract_dir + '/**/lspop*', recursive=True) if os.path.isdir(x)]
            conv = self.run_tasks(self.convert_esri_grid_to_geotiff, conv_list)
            self.log_run(conv)





def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
        "raw_dir": Path(config["main"]["raw_dir"]),
        "output_dir": Path(config["main"]["output_dir"]),
        "years": [int(y) for y in config["main"]["years"].split(", ")],
        "run_download": config["main"].getboolean("run_download"),
        "run_extract": config["main"].getboolean("run_extract"),
        "run_conversion": config["main"].getboolean("run_conversion"),
        "cookie_name": config["main"]["cookie_name"],
        "cookie_value": config["main"]["cookie_value"],
        "username": config["main"]["username"],
        "password": config["main"]["password"],
        "overwrite_download": config["main"].getboolean("overwrite_download"),
        "overwrite_extract": config["main"].getboolean("overwrite_extract"),
        "overwrite_conversion": config["main"].getboolean("overwrite_conversion"),
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


    class_instance = LandScanPop(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["run_download"], config_dict["run_extract"], config_dict["run_conversion"], config_dict["cookie_name"], config_dict["cookie_value"], config_dict["username"], config_dict["password"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_conversion"])

    class_instance.run(backend=config_dict["backend"], task_runner=config_dict["task_runner"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], log_dir=timestamp_log_dir)
