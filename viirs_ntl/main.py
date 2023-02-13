# data download for viirs nighttime lights data
# data from: https://eogdata.mines.edu/nighttime_light/ 

import os
import sys
from pathlib import Path
from datetime import datetime
from urllib import request, parse
from configparser import ConfigParser
from typing import List
import requests
import json
import subprocess

import rasterio
import numpy as np

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset

class VIIRS_NTL(Dataset):

    name = "DVNL"

    def __init__(self, raw_dir, output_dir, months, years, username, password, client_secret, annual=True, overwrite_download=False, overwrite_extract=False, overwrite_processing=False ):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.months = months
        self.years = years
        self.username = username
        self.password = password
        self.client_secret = client_secret
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
    
    def runcmd(cmd, verbose = False, *args, **kwargs):
        """""
        from: https://www.scrapingbee.com/blog/python-wget/
        """""

        process = subprocess.Popen(
            cmd,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            text = True,
            shell = True
        )
        std_out, std_err = process.communicate()
        if verbose:
            print(std_out.strip(), std_err)
        pass

    
    def manage_download(self, year):
        # consider doing separate directories for years when doing monthly data download
        """
        Download individual file
        """
        logger = self.get_logger()

        logger.info("Retrieving token")
        token = self.get_token()

        if self.annual:
            download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}"
            download_dest = download_url.format(YEAR = year)
            local_filename = self.raw_dir / f"raw_viirs_ntl_{year}"
        else:
            download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/"
            download_dest = download_url.format(YEAR = year, MONTH = month)
            local_filename = self.raw_dir / f"raw_viirs_ntl_{year}_{month}"
        
        if local_filename.exists() and not self.overwrite_download:
            logger.info(f"Download Exists: {download_dest}")
        else:
            logger.debug(f"Attempting to download: {str(download_dest)}")
            try:
                # wget -c -m -np -nH --cut-dirs=1 --header "Authorization: Bearer ${access_token}" -P ${out_dir} "${year_url}/${y}" -R .html -A .tif.gz
                temp_command = "wget -c -m -np -nH --cut-dirs=1 --header \"Authorization: Bearer {TOKEN}\" -P {OUT_DIR} \"{DWN_URL}\" -R .html -A .tif.gz"
                command = temp_command.format(TOKEN = token, OUT_DIR = self.raw_dir, DWN_URL = download_dest)
                self.runcmd(command, verbose = True)
            except:
                logger.info(f"Failed to download: {str(download_dest)}")
            else:
                logger.info(f"Downloaded {str(download_dest)}")

        return (download_dest, command)

        

    def main(self):
        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        os.makedirs(self.raw_dir, exist_ok=True)

        logger.info("Running data download")
        if self.annual:
            download = self.run_tasks(self.manage_download, [[y] for y in self.years])
        self.log_run(download)


    
def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "annual" :  config["main"].getboolean("annual"),
            "years": [int(y) for y in config["main"]["years"].split(", ")],
            "months": [int(y) for y in config["main"]["months"].split(", ")],
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "log_dir": Path(config["main"]["raw_dir"]) / "logs",
            "overwrite_download": config["main"].getboolean("overwrite_download"),
            "overwrite_extract": config["main"].getboolean("overwrite_extract"),
            "overwrite_processing": config["main"].getboolean("overwrite_processing"),

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

    class_instance = VIIRS_NTL(config_dict["raw_dir"], config_dict["output_dir"], config_dict["months"], config_dict["years"], config_dict["username"], config_dict["password"], config_dict["client_secret"], config_dict["annual"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])