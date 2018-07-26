"""Create list of files to download (see example_file_list.txt)

Then run below to download (with earthdata user/pass login info)

wget -L --user=youraccountemail --password=yourpassword --load-cookies ~/.cookies --save-cookies ~/.cookies -i filelist.txt

"""

import os
import errno
import requests
from bs4 import BeautifulSoup
import time
import datetime


root_url = "https://e4ftl01.cr.usgs.gov"

data_url = os.path.join(root_url, "MOLT/MOD11C3.006")


timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y_%m_%d')

output_dir = "/sciclone/aiddata10/REU/geo/raw/modis_lst"

output_file = os.path.join(output_dir, "file_list_{}.txt".format(timestamp))



def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def listFD(url, ext=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    urllist = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return urllist


make_dir(output_dir)


output_str = ""
for file in listFD(data_url):

    hdfurl = listFD(file, 'hdf')

    if len(hdfurl) == 0:
        print "Cannot download: \n{}".format(file)
        continue

    output_str += hdfurl[0] + '\n'


with open(output_file, 'wb') as dst:
    dst.write(output_str)
