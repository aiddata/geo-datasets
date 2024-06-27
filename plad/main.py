# data download script for PLAD political leaders' birthplace dataset
# info link: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YUS575 

import os
import sys
import requests
from pathlib import Path
from configparser import ConfigParser
import xlrd
import pandas as pd

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))

from dataset import Dataset

class PLAD(Dataset):

    name = "PLAD"

    def __init__(self, raw_dir, output_dir, years, leader_options, max_retries, overwrite_download=False, overwrite_sorting=False):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.leader_options = leader_options
        self.max_retries = max_retries
        self.overwrite_download = overwrite_download
        self.overwrite_sorting = overwrite_sorting
        self.download_url = "https://dataverse.harvard.edu/api/access/datafile/5211722"

    def test_connection(self):
        # test connection
        test_request = requests.get("https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YUS575", verify=True)
        test_request.raise_for_status()

    def manage_download(self):
        """
        Download original spreadsheet
        """

        logger = self.get_logger()

        download_dest = "https://dataverse.harvard.edu/api/access/datafile/5211722"
        local_filename = self.raw_dir / "plad.xls"

        if os.path.isfile(local_filename) and not self.overwrite_download:
            logger.info(f"Download Exists: {local_filename}")
        else:
            attempts = 1
            while attempts <= self.max_retries:
                try:
                    with requests.get(download_dest, stream=True, verify=True) as r:
                        r.raise_for_status()
                        with open(local_filename, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024):
                                f.write(chunk)
                    logger.info(f"Downloaded: {download_dest}")
                    return (download_dest, local_filename)
                except Exception as e:
                    attempts += 1
                    if attempts > self.max_retries:
                        logger.info(str(e) + f": Failed to download: {str(download_dest)}")
                        return (download_dest, local_filename)
                    else:
                        logger.info(f"Attempt " + {str(attempts)} + ": "+ {str(download_dest)})
    
    def sort_data(self, year, option):
        "create csv for each year + leader option in config"

        logger = self.get_logger()

        output_filename = "leader_birthplace_data_{YEAR}_{OPTION}.csv".format(YEAR = year, OPTION = option)
        output_path = self.output_dir / output_filename
        if os.path.isfile(output_path) and not self.overwrite_sorting:
            logger.info(f"File exists: {str(output_path)}")
            return ("File exists", str(output_path))

        master_data = self.raw_dir / "plad.xls"
        if not os.path.isfile(master_data):
            logger.info(f"Error: Master data download: {master_data} not found" )
            raise Exception(f"Data file not found: {master_data}")
        data_sheet = xlrd.open_workbook(str(master_data))
        data = data_sheet.sheet_by_index(0)
        
        return_list = []
        for row in range(1, data.nrows):
            country_code = data.cell_value(row, 0) # code or name?
            leader_name = data.cell_value(row, 1)
            lat = data.cell_value(row, 13)
            long = data.cell_value(row, 14)

            start_year = int(data.cell_value(row, 6))
            end_year = int(data.cell_value(row, 7))
            if (year > start_year) & (year < end_year):
                return_list.append((country_code, leader_name, lat, long))
            elif ("outgoing" in option) & (year == end_year):
                return_list.append((country_code, leader_name, lat, long))
            elif ("incoming" in option) & (year == start_year):
                return_list.append((country_code, leader_name, lat, long))

        try:
            data_df = pd.DataFrame(return_list, columns=["country", "leader_name", "latitude", "longitude"])
            data_df.to_csv(output_path, index=False)
            logger.info(f"Data Compiled: {str(output_path)}")
            return ("Success", output_path)
        except Exception as e:
            logger.info(f"Error compiling {output_path}:   {str(e)}")
            raise Exception(str(e), output_path)

    def main(self):

        logger = self.get_logger()

        os.makedirs(self.raw_dir, exist_ok=True)

        logger.info("Testing Connection...")
        self.test_connection()

        logger.info("Running data download")
        self.manage_download()

        os.makedirs(self.output_dir, exist_ok=True)

        logger.info("Sorting Data")
        sort = self.run_tasks(self.sort_data, [[y,o] for y in self.years for o in self.leader_options])
        self.log_run(sort)

def get_config_dict(config_file="config.ini"):
    config = ConfigParser()
    config.read(config_file)

    return {
            "raw_dir": Path(config["main"]["raw_dir"]),
            "output_dir": Path(config["main"]["output_dir"]),
            "years": [int(y) for y in config["main"]["years"].split(", ")],
            "leader_options": [str(y) for y in config["main"]["leader_options"].split(", ")],
            "log_dir": Path(config["main"]["output_dir"]) / "logs", 
            "backend": config["run"]["backend"],
            "task_runner": config["run"]["task_runner"],
            "run_parallel": config["run"].getboolean("run_parallel"),
            "max_workers": int(config["run"]["max_workers"]),
            "max_retries": config["main"].getint("max_retries"),
            "cores_per_process": int(config["run"]["cores_per_process"]),
            "overwrite_download": config["main"].getboolean("overwrite_download"),
            "overwrite_sorting": config["main"].getboolean("overwrite_sorting"),
        }

if __name__ == "__main__":
    config_dict = get_config_dict()

    class_instance = PLAD(config_dict["raw_dir"], config_dict["output_dir"], config_dict["years"], config_dict["leader_options"], config_dict["max_retries"], config_dict["overwrite_download"], config_dict["overwrite_sorting"])

    class_instance.run(backend=config_dict["backend"], run_parallel=config_dict["run_parallel"], max_workers=config_dict["max_workers"], task_runner=config_dict["task_runner"], log_dir=config_dict["log_dir"])
