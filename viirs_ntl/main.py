# data download for viirs nighttime lights data
# data from: https://eogdata.mines.edu/nighttime_light/ 

import os
import sys
from pathlib import Path
from configparser import ConfigParser
import requests
import json
import gzip
import shutil

from bs4 import BeautifulSoup

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset

class VIIRS_NTL(Dataset):

    name = "VIIRS_NTL"

    def __init__(self, raw_dir, output_dir, annual_files, monthly_files, months, years, username, password, client_secret, max_retries, annual=True, overwrite_download=False, overwrite_extract=False, overwrite_processing=False):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.annual_files = annual_files
        self.monthly_files = monthly_files
        self.months = months
        self.years = years
        self.username = username
        self.password = password
        self.client_secret = client_secret
        self.max_retries = max_retries
        self.annual = annual
        self.overwrite_download = overwrite_download
        self.overwrite_extract = overwrite_extract
        self.overwrite_processing = overwrite_processing

    def test_connection(self):
        # test connection
        test_request = requests.get("https://eogdata.mines.edu/nighttime_light/", verify=True)
        test_request.raise_for_status()

    
    def get_token(self):
        """
        retrieves access token for data download
        """
        params = {
            'client_id' : 'eogdata_oidc',
            'client_secret' : self.client_secret,
            'username' : self.username,
            'password' : self.password,
            'grant_type' : 'password'
        }

        token_url = 'https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token'
        response = requests.post(token_url, data = params)
        access_token_dict = json.loads(response.text)
        access_token = access_token_dict.get('access_token')

        return access_token


    
    def manage_download(self, year, file, month=None):
        # consider doing separate directories for years when doing monthly data download
        """
        Download individual file
        """
        logger = self.get_logger()

        logger.info("Retrieving token")
        token = self.get_token()
        headers = {
        "Authorization": f"Bearer {token}",
        }

        if self.annual:
            if year == 2012:
                if (file == "average_masked") | (file == "lit_mask") | (file =="median_masked"):
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}04-201303_global_vcmcfg_c202102150000.{TYPE}.tif.gz"
                else:
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}04-201303_global_vcmcfg_c202101211500.{TYPE}.tif.gz"
            elif year == 2013:
                if (file == "average_masked") | (file == "lit_mask") | (file =="median_masked"):
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmcfg_cc202102150000..{TYPE}.tif.gz"
                else:
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmcfg_c202101211500.{TYPE}.tif.gz"
            elif year == 2021:
                download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/2021/VNL_v2_npp_{YEAR}_global_vcmslcfg_c202203152300.{TYPE}.tif.gz"
            else:
                if (file == "average_masked") | (file == "lit_mask") | (file == "median_masked"):
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmslcfg_c202102150000.{TYPE}.tif.gz"
                else:
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmslcfg_c202101211500.{TYPE}.tif.gz"
            download_dest = download_url.format(YEAR = year, TYPE = file)
            local_filename = self.raw_dir / f"raw_viirs_ntl_{year}_{file}.tif.gz"

        else:
            # consider: make separate directories for each year's monthly data
            if (year == 2022) & (month == 8):
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/NOAA-20/vcmcfg/"
            else:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmcfg/"
            
            if month < 10:
                # file directory formatting
                month = "0" + str(month)
            
            download_url = download_url.format(YEAR = year, MONTH = month)
            
            attempts = 1
            while attempts <= self.max_retries:
                try:
                    session = requests.Session()
                    r = session.get(download_url, headers={'User-Agent': 'Mozilla/5.0'})
                    soup = BeautifulSoup(r.content, 'html.parser')

                    items = soup.find_all("tr", {"class": "odd"})
                    link_list = []

                    for i in items:
                        link = str(i.findChild("a")['href'])
                        link_list.append(link)
                    
                    file_type = file + ".tif.gz"
                    file_code = ""
                    for link in link_list:
                        if file_type in link:
                            file_code = link
                except Exception as e:
                    attempts += 1
                    if attempts > self.max_retries:
                        raise Exception(str(e) + f": Failed to download: {str(download_dest)}" )
                else:
                    logger.info(f"Retrieved: {file_code}")
                    break
                
            if len(file_code) == 0:
                logger.info("Download option does not exist yet: " + str(year) + "/" + str(month) + "/ " + file)
                raise Exception("Download option does not exist yet: " + str(year) + "/" + str(month) + "/" + file)
                
            download_dest = download_url + str(file_code)
            local_filename = self.raw_dir / f"raw_viirs_ntl_{year}_{month}_{file}"
        
        if local_filename.exists() and not self.overwrite_download:
            logger.info(f"Download Exists: {local_filename}")
        else:
            try:
                with requests.get(download_dest, headers=headers, stream=True) as src:
                    # raise an exception (fail this task) if HTTP response indicates that an error occured
                    src.raise_for_status()
                    with open(local_filename, "wb") as dst:
                        dst.write(src.content)
            except Exception as e:
                logger.info(f"Failed to download: {str(download_dest)}")
                raise Exception(str(e) + f": Failed to download: {str(download_dest)}" )
            else:
                logger.info(f"Downloaded {str(local_filename)}")

        return (download_dest, local_filename)
    
    def extract_files(self, year, file, month=None):
        """
        Extract individual file
        """
        logger = self.get_logger()
        if self.annual:
            raw_local_filename = self.raw_dir / f"raw_viirs_ntl_{year}_{file}.tif.gz"
            output_filename = self.output_dir / f"raw_extracted_viirs_ntl_{year}_{file}.tif.gz"
        else:
            raw_local_filename = self.raw_dir / f"raw_viirs_ntl_{year}_{month}_{file}"
            output_filename = self.output_dir / f"raw_extracted_viirs_ntl_{year}_{month}_{file}"
        if output_filename.exists() and not self.overwrite_extract:
            logger.info(f"Extracted File Exists: {output_filename}")
            return (raw_local_filename, output_filename)
        else:
            try:
                with gzip.open(raw_local_filename, 'rb') as f_in:
                    with open(output_filename, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                return (raw_local_filename, output_filename)
            except Exception as e:
                logger.info(f"Failed to extract: {str(raw_local_filename)}")
                raise Exception(str(e) + ": " f"Failed to extract: {str(raw_local_filename)}")


        

    def main(self):
        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        os.makedirs(self.raw_dir, exist_ok=True)
        logger.info("Running data download")
        if self.annual:
            download = self.run_tasks(self.manage_download, [[y, f] for y in self.years for f in self.annual_files])
        else:
            download = self.run_tasks(self.manage_download, [[y, f, m] for y in self.years for f in self.monthly_files for m in self.months])
        self.log_run(download)

        os.makedirs(self.output_dir, exist_ok=True)
        logger.info("Extracting raw files")
        if self.annual:
            extraction = self.run_tasks(self.extract_files, [[y, f] for y in self.years for f in self.annual_files])
        else:
            extraction = self.run_tasks(self.extract_files, [[y, f, m] for y in self.years for f in self.monthly_files for m in self.months])
        self.log_run(extraction)

    
def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "annual" :  config["main"].getboolean("annual"),
            "years": [int(y) for y in config["main"]["years"].split(", ")],
            "months": [int(y) for y in config["main"]["months"].split(", ")],
            "annual_files": [str(y) for y in config["main"]["annual_files"].split(", ")],
            "monthly_files": [str(y) for y in config["main"]["monthly_files"].split(", ")],
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "log_dir": Path(config["main"]["raw_dir"]) / "logs",
            "overwrite_download": config["main"].getboolean("overwrite_download"),
            "overwrite_extract": config["main"].getboolean("overwrite_extract"),
            "overwrite_processing": config["main"].getboolean("overwrite_processing"),
            "max_retries": config["main"].getint("max_retries"),

            "username" : config["download"]["username"],
            "password" : config["download"]["password"],
            "client_secret" : config["download"]["client_secret"],

            "backend": config["run"]["backend"],
            "task_runner": config["run"]["task_runner"],
            "run_parallel": config["run"].getboolean("run_parallel"),
            "max_workers": int(config["run"]["max_workers"])
            
        }

if __name__ == "__main__":
    config_dict = get_config_dict()

    class_instance = VIIRS_NTL(config_dict["raw_dir"], config_dict["output_dir"], config_dict["annual_files"], config_dict["monthly_files"], config_dict["months"], config_dict["years"], config_dict["username"], config_dict["password"], config_dict["client_secret"], config_dict["max_retries"], config_dict["annual"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])