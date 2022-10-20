#!/usr/bin/env python

#
# details: data download yearly/monthy viirs black marble ntl data from
# https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/products/VNP46A3/
# https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/products/VNP46A4/
#
# download function based on NASA earthdata's sample automated script: https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/data-download-scripts/#appkey
#

import os
import sys
import time
from pathlib import Path
from datetime import datetime
import ssl
from urllib.request import urlopen, Request
import sys

import pandas as pd



def read_remote_csv(url, retry=10):
    """Read CSV into Pandas from URL with automatic retries"""
    attempts = 0
    max_attempts = retry
    while attempts < max_attempts:
        try:
            df = pd.read_csv(url)
            return df
        except:
            attempts += 1
            time.sleep(60)

    raise Exception(f'Could not read remote CSV ({url}')


def geturl(url, dst_path, token):
    """Get content from URL using authentication token and write to file
    """
    USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')

    headers = { 'user-agent' : USERAGENT }
    headers['Authorization'] = 'Bearer ' + token

    CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    fh = urlopen(Request(url, headers=headers), context=CTX)
    with open(dst_path, 'b+w') as f:
        f.write(fh.read())


def build_download_list(year_list, mode, results_dir):

    datasets = {
        'monthly': 'VNP46A3',
        'yearly': 'VNP46A4'
    }

    data_id = datasets[mode]

    base_download_url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/{data_id}"

    download_task_list = []

    product_csv_download_url = f'{base_download_url}.csv'
    product_year_list_df = read_remote_csv(product_csv_download_url)
    product_year_list = product_year_list_df.name.to_list()

    (results_dir / 'data' / 'file_lists' / mode).mkdir(parents=True, exist_ok=True)

    active_year_list = [i for i in product_year_list if i in year_list]
    for year in active_year_list:
        year_dir = results_dir / 'data' / 'h5_tiles' / mode / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        day_list_path = results_dir / 'data' / 'file_lists' / mode /f'{year}_months.csv'

        update_day_list = True
        if day_list_path.is_file():
            day_list_df = pd.read_csv(day_list_path)
            update_day_list = False
            if mode == 'monthly' and len(day_list_df) < 12:
                update_day_list = True

        if update_day_list:
            year_csv_download_url = f'{base_download_url}/{year}.csv'
            day_list_df = read_remote_csv(year_csv_download_url)
            day_list_df.to_csv(day_list_path)


        day_list_df['day'] = day_list_df.name.apply(lambda x: str(x).zfill(3))
        day_list_df['month'] = day_list_df.name.apply(lambda x: datetime.strptime(f"{year}-{x}", "%Y-%j").strftime("%m"))

        daymonth_list = zip(day_list_df.day, day_list_df.month)

        for day, month in daymonth_list:

            file_list_path = results_dir / 'data' / 'file_lists' / mode / f'{year}-{month}_files.csv'

            if file_list_path.is_file():
                file_list_df = pd.read_csv(file_list_path)
            else:
                month_csv_download_url = f'{base_download_url}/{year}/{day}.csv'
                file_list_df = read_remote_csv(month_csv_download_url)
                file_list_df.to_csv(file_list_path)


            file_list_df['dl_url'] = file_list_df.name.apply(lambda x: f'{base_download_url}/{year}/{day}/{x}')

            if mode == 'yearly':
                file_list_df['dl_path'] = file_list_df.name.apply(lambda x: f'{year_dir}/{x}')
            else:
                file_list_df['dl_path'] = file_list_df.name.apply(lambda x: f'{year_dir}/{month}/{x}')


            tmp_download_task_list = list(map(list, zip(
                file_list_df["dl_url"],
                file_list_df["dl_path"]
            )))

            download_task_list.extend(tmp_download_task_list)

    return download_task_list



# add a file size check based on file CSV from web?
def manage_download(download_url, output_dest, token):
    try:
        if not os.path.exists(output_dest):
            print('downloading: ' , output_dest)
            Path(output_dest).parent.mkdir(parents=True, exist_ok=True)
            geturl(download_url, output_dest, token)
            return (0, "Downloaded", download_url, output_dest)
        else:
            print('skipping: ', output_dest)
            return (0, "Exists", download_url, output_dest)

    except IOError as e:
        # print("IOError: open `%s': %s" % (e.filename, e.strerror), file=sys.stderr)
        return (1, repr(e), download_url, output_dest)
    except Exception as e:
        return (1, repr(e), download_url, output_dest)

