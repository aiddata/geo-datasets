
import os
from ftplib import FTP
import errno

def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


ftp = FTP('arthurhou.pps.eosdis.nasa.gov')
ftp.login(user="zlv@aiddata.wm.edu", passwd="zlv@aiddata.wm.edu")

ftp.cwd('gpmdata')
rootdir = ftp.pwd()

# data folder format: ftp://arthurhou.pps.eosdis.nasa.gov/gpmdata/2014/05/03/imerg/
# file name: 3B-MO.MS.MRG.3IMERG.20140501-S000000-E235959.05.V05B.HDF5
# 3B-HHR.MS.MRG.3IMERG.20140501-S233000-E235959.1410.V05B.HDF5


output_dir = "/sciclone/aiddata10/REU/geo/raw/gpm"

make_dir(output_dir)

years = map(str, range(2014, 2019))

months = [i.zfill(2) for i in map(str, range(1,13))]

# monthly data is saved in the first date's folder
dates = ['01']


for year in years:

    for month in months:

        # on ftp: geotiff data in "gis" dir, hdf5 in "imerg" dir
        filepath = os.path.join(rootdir, year, month, dates[0], 'gis')

        try:

            ftp.cwd(filepath)
            filelist = ftp.nlst()

            for file in filelist:

                if "3B-MO" in file:

                    print file
                    #ftp.retrbinary('RETR %s' % filepath, file.write)

                    try:

                        local_filename = os.path.join(output_dir, file)
                        lf = open(local_filename, "wb")
                        ftp.retrbinary("RETR " + file, lf.write)
                        lf.close()

                    except:
                         print "cannot download file: {}".format(file)


        except:

            print "no data for: {0} {1}".format(year, month)














