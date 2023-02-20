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

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset

class VIIRS_NTL(Dataset):

    name = "VIIRS_NTL"

    def __init__(self, raw_dir, output_dir, annual_files, monthly_files, months, years, username, password, client_secret, annual=True, overwrite_download=False, overwrite_extract=False, overwrite_processing=False ):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.annual_files = annual_files
        self.monthly_files = monthly_files
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
                if (type == "average_masked") | (type == "lit_mask") | (type =="median_masked"):
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}04-201303_global_vcmcfg_c20210121150000.{TYPE}.tif.gz"
                else:
                    download_url = download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}04-201303_global_vcmcfg_c202101211500.{TYPE}.tif.gz"
            elif year == 2013:
                if (type == "average_masked") | (type == "lit_mask") | (type =="median_masked"):
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmcfg_c20210121150000.{TYPE}.tif.gz"
                else:
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmcfg_c202101211500.{TYPE}.tif.gz"
            elif year == 2021:
                download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/2021/VNL_v2_npp_{YEAR}_global_vcmslcfg_c202203152300.{TYPE}.tif.gz"
            else:
                if (type == "average_masked") | (type == "lit_mask") | (type =="median_masked"):
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmslcfg_c20210121150000.{TYPE}.tif.gz"
                else:
                    download_url = "https://eogdata.mines.edu/nighttime_light/annual/v20/{YEAR}/VNL_v2_npp_{YEAR}_global_vcmslcfg_c202101211500.{TYPE}.tif.gz"
            download_dest = download_url.format(YEAR = year, TYPE = file)
            local_filename = self.raw_dir / f"raw_viirs_ntl_{year}_{file}.tif.gz"

        else:
            # consider: make separate directories for each year's monthly data
            if year == 2012:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 4:
                    end_day = 30
                    file_code = 201605121456
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201605121458
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201605121459
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 201605121509
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201602121348
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201602090953
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201602051401
                elif month == 11:
                    end_day = 30
                    file_code = 201601270845
                elif month == 12:
                    end_day = 31
                    file_code = 201601041440
                else: 
                    # for file types that don't exist
                    logger.info("Download option does not exist yet: " + str(year) + "/" + str(month) + "/ " + file)
                    raise Exception("Download option does not exist yet: " + str(year) + "/" + str(month) + "/" + file)

            elif year == 2013:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 201605121529
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 201605131247
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 201605131250
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 201605131251
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201605131256
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201605131304
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 201605131305
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201605131312
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201605131325
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201605131331
                elif month == 11:
                    end_day = 30
                    file_code = 201605131332
                elif month == 12:
                    end_day = 31
                    file_code = 201605131341
                    
            elif year == 2014:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 2015006171539
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 201507201053
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 201506121552
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 201507201613
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201502061154
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201502121209
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 2015006231100
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201508131500
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201502251402
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201502200936
                elif month == 11:
                    end_day = 30
                    file_code = 201502231456
                elif month == 12:
                    end_day = 31
                    file_code = 201502231126
            
            elif year == 2015:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 201505111710
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 201504281527
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 201505191919
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 201506011709
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201506161326
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201508141523
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 201509151840
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201509301759
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201511121210
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201511181405
                elif month == 11:
                    end_day = 30
                    file_code = 201512121649
                elif month == 12:
                    end_day = 31
                    file_code = 201601251413
            
            elif year == 2016:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 201603132032
                    month = "01"
                elif month == 2:
                    end_day = 29
                    file_code = 201603152010
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 201604191144
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 201606140957
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201606281430
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201608101833
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 201609121310
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201610041111
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201610280945
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201612011125
                elif month == 11:
                    end_day = 30
                    file_code = 201612191237
                elif month == 12:
                    end_day = 31
                    file_code = 201701271138
            
            elif year == 2017:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 201702241225
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 201703012030
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 201705020851
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 201705011300
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201706021300
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201707021700
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 201708061200
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201709051000
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201710041620
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201711021230
                elif month == 11:
                    end_day = 30
                    file_code = 201712040930
                elif month == 12:
                    end_day = 31
                    file_code = 201801021747
            
            elif year == 2018:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 201805221252
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 201803012000
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 201804022005
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 201805021400
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201806061100
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201904251200
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 201812111300
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201809070900
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201810250900
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201811131000
                elif month == 11:
                    end_day = 30
                    file_code = 201812081230
                elif month == 12:
                    end_day = 31
                    file_code = 201902122100
            
            elif year == 2019:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 201905201300
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 201903110900
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 201904071900
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 201905191000
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 201906130930
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 201907091100
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 201908090900
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 201909051300
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 201910062300
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 201911061400
                elif month == 11:
                    end_day = 30
                    file_code = 201912131600
                elif month == 12:
                    end_day = 31
                    file_code = 202001140900
            
            elif year == 2020:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if month == 1:
                    end_day = 31
                    file_code = 202002111500
                    month = "01"
                elif month == 2:
                    end_day = 29
                    file_code = 202003021200
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 202007042300
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 202006121200
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 202006221000
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 202008012300
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 202008142300
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 202009111000
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 202010082300
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 202011050900
                elif month == 11:
                    end_day = 30
                    file_code = 202012092300
                elif month == 12:
                    end_day = 31
                    file_code = 202101130900
            
            elif year == 2021:
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if file == "avg_rade9h.masked":
                    logger.info("Download option does not exist yet: " + str(year) + "/" + str(month) + "/ " + file)
                    raise Exception("Download option does not exist yet: " + str(year) + "/" + str(month) + "/" + file)

                elif month == 1:
                    end_day = 31
                    file_code = 202102062300
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 202103091200
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 202104061200
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 202105062200
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 202106060700
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 202106060700
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 202108071200
                    month = "07"
                elif month == 8:
                    end_day = 31
                    file_code = 202109141100
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 202110112300
                    month = "09"
                elif month == 10:
                    end_day = 31
                    file_code = 202111062300
                elif month == 11:
                    end_day = 30
                    file_code = 202112060900
                elif month == 12:
                    end_day = 31
                    file_code = 202201100700

            else: # year = 2022
                
                download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/vcmslcfg/SVDNB_npp_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"

                if file == "avg_rade9h.masked":
                    logger.info("Download option does not exist yet: " + str(year) + "/" + str(month) + "/ " + file)
                    raise Exception("Download option does not exist yet: " + str(year) + "/" + str(month) + "/" + file)

                elif month == 1:
                    end_day = 31
                    file_code = 202202041600
                    month = "01"
                elif month == 2:
                    end_day = 28
                    file_code = 202203052300
                    month = "02"
                elif month == 3:
                    end_day = 31
                    file_code = 202204080900
                    month = "03"
                elif month == 4:
                    end_day = 30
                    file_code = 202205051100
                    month = "04"
                elif month == 5:
                    end_day = 31
                    file_code = 202206071200
                    month = "05"
                elif month == 6:
                    end_day = 30
                    file_code = 202207141300
                    month = "06"
                elif month == 7:
                    end_day = 31
                    file_code = 202208102300
                    month = "07"
                elif month == 8:
                    # this file has unique download link
                    download_url = "https://eogdata.mines.edu/nighttime_light/monthly_notile/v10/{YEAR}/{YEAR}{MONTH}/NOAA-20/vcmslcfg/SVDNB_j01_{YEAR}{MONTH}01-{YEAR}{MONTH}{MED}_global_vcmslcfg_v10_c{FCODE}.{TYPE}.tif.gz"
                    end_day = 31
                    file_code = 202209231200
                    month = "08"
                elif month == 9:
                    end_day = 30
                    file_code = 202210122300
                    month = "09"
                else:
                    logger.info("Download option does not exist yet: " + str(year) + "/" + str(month) + "/ " + file)
                    raise Exception("Download option does not exist yet: " + str(year) + "/" + str(month) + "/" + file)

            download_dest = download_url.format(YEAR = year, MONTH = month,TYPE = file, MED = end_day, FCODE = file_code)
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
                raise e
            else:
                logger.info(f"Downloaded {str(local_filename)}")

        return (download_dest, local_filename)

        

    def main(self):
        logger = self.get_logger()

        logger.info("Testing Connection...")
        self.test_connection()

        os.makedirs(self.raw_dir, exist_ok=True)

        # add iterative loop to prep tuple for imput parameter

        logger.info("Running data download")
        if self.annual:
            download = self.run_tasks(self.manage_download, [[y, f] for y in self.years for f in self.annual_files])
        else:
            download = self.run_tasks(self.manage_download, [[y, f, m] for y in self.years for f in self.monthly_files for m in self.months])
        self.log_run(download)


    
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

    class_instance = VIIRS_NTL(config_dict["raw_dir"], config_dict["output_dir"], config_dict["annual_files"], config_dict["monthly_files"], config_dict["months"], config_dict["years"], config_dict["username"], config_dict["password"], config_dict["client_secret"], config_dict["annual"], config_dict["overwrite_download"], config_dict["overwrite_extract"], config_dict["overwrite_processing"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])