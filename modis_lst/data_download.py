


from bs4 import BeautifulSoup
import requests
import os
import urllib2
import urllib
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

import urlparse


root = r"https://e4ftl01.cr.usgs.gov"
rooturl = r"https://e4ftl01.cr.usgs.gov/MOLT/MOD11C3.006"
outfolder = r'/Users/miranda/Documents/AidData/projects/datasets/MODIS_temp'
#outfolder = r'/sciclone/aiddata10/REU/pre_geo/modis_temp/rawdata'

#username = "zlv@aiddata.wm.edu"
#password = mypassword, 3 Cap with 3 #



def listFD(url, ext=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    urllist = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]

    return urllist



def retrieve_data(urls, username, password, outf):

    r = requests.get(urls, auth=HTTPDigestAuth(username, password)) #HTTPDigestAuth(username, password)
    page = r.content

    print page


    #response = urllib2.urlopen(urls)
    #html = response.read()

    """
    p = urllib2.HTTPPasswordMgrWithDefaultRealm()
    p.add_password(None, urls, usr, pswd)
    handler = urllib2.HTTPBasicAuthHandler(p)
    opener = urllib2.build_opener(handler)
    opener.open(urls)
    urllib2.install_opener(opener)

    req = urllib2.Request(urls)
    response = urllib2.urlopen(req)
    """


    ##lf = open(outf, 'wb')
    #lf.write(page)
    #lf.close()



filelist = list()

for file in listFD(rooturl):

    hdfurl = listFD(file, 'hdf')

    if len(hdfurl) != 0:

        print "start working on", hdfurl[0]
        foldername = hdfurl[0].split('/')[-3]
        filename = hdfurl[0].split('/')[-1]

        filelist.append(hdfurl[0])


        if os.path.exists(foldername):
            os.chdir(os.path.join(outfolder, foldername))

            retrieve_data(hdfurl[0], username, password, filename)
            #retrieve_data(hdfurl[0], filename)


        else:
            f = os.path.join(outfolder, foldername)
            os.mkdir(f)
            os.chdir(os.path.join(outfolder, foldername))

            retrieve_data(hdfurl[0], username, password, filename)
            #retrieve_data(hdfurl[0], filename)


    else:
        print "not downloadable"



with open(outtxt, 'wb') as txt:

    for f in filelist:

        txt.write(f + '\n')






