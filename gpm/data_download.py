


from ftplib import FTP
import os

ftp = FTP('arthurhou.pps.eosdis.nasa.gov')
ftp.login(user="zlv@aiddata.wm.edu", passwd="zlv@aiddata.wm.edu")

ftp.cwd('gpmdata')
rootdir = ftp.pwd()


# sciclone directory
out = r'/sciclone/aiddata10/REU/pre_geo/GPM/raw/imerg'
#data_type = 'imerg' # imerg
#outfolder = os.path.join(out,data_type)


# data folder format: ftp://arthurhou.pps.eosdis.nasa.gov/gpmdata/2014/05/03/imerg/
# file name: 3B-MO.MS.MRG.3IMERG.20140501-S000000-E235959.05.V05B.HDF5
# 3B-HHR.MS.MRG.3IMERG.20140501-S233000-E235959.1410.V05B.HDF5
years = ['2014','2015', '2016', '2017','2018']

months = ['01', '02', '03', '04', '05', '06',
         '07', '08', '09', '10', '11', '12']
#months = ['01']
dates = ['01'] # it seems the monthly data is saved in the first date's folder, double check the document

sub_dirs = list()

for year in years:

    for month in months:

        name = "/".join([year, month, dates[0], 'imerg'])

        filepath = os.path.join(rootdir, name)

        #sub_dirs.append(filepath)



        try:

            ftp.cwd(filepath)
            filelist = ftp.nlst()
            for file in filelist:
		
                if "3B-MO" in file:

                    #ftp.retrbinary('RETR %s' % filepath, file.write)

                    local_filename = os.path.join(outfolder, file)
                    lf = open(local_filename, "wb")
                    ftp.retrbinary("RETR " + file, lf.write)
                    lf.close()


        except:

            print "no directory exist", filepath














