from mpi4py import MPI
import subprocess as sp
import sys
import os

runscript = sys.argv[1]


comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

# base path where year/day directories are located
# data downloaded using wget (same wget call can be used to download new data)
# wget -r -c -N ftp://gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/
path_base = "/sciclone/aiddata10/REU/raw/gimms.gsfc.nasa.gov/MODIS/std/GMOD09Q1/tif/NDVI/"

# list of years to ignore
ignore = ['2000','2001','2002','2003','2004','2005','2006']
# ignores self
# ignore.append(path_base[path_base.rindex('/')+1:])

# list of all [year, day] combos
qlist = []

# qlist += [['2001','169'],['2005','257']]

# get years
# years = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name)) and name not in ignore]

# use limited years for testing 
years = ['2001']


for year in years:

	# get days for year
	path_year = path_base + year
	days = [name for name in os.listdir(path_year) if os.path.isdir(os.path.join(path_year, name))]

	# use limited days for testing 
	# days = ['001','009']
	# days = ['001','009','017','025','033','041']
	# days = ['001','009','017','025','033','041','049','057']
	# days = ['065','073','081','089','097','105','113','121']
	# days = ['001','009','017','025','033','041','049','057','065','073','081','089','097','105','113','121']

	# days = ['001','009','017','025']
	# days = ['033','041','049','057','065','073']
	# days = ['081','089','097','105','113','121','129','137']
	# days = ['145','153','161','169','177','185','193','201','209','217','225','233','241','249','257','265']

	# days = ['305','313','321','329','337','345','353','361']

	# days = ['129','137','145','153','161','169','177','185','193','201','209','217','225','233','241','249','257','265','273','281','289','297','305','313']
	# days = ['321','329','337','345','353','361']

	days = ['169','177']

	# days = ['001','009','017','025','033','041','049','057','065','073','081','089','097','105','113','121','129','137','145','153','161','169','177','185','193','201','209','217','225','233','241','249']

	qlist += [[year,day] for day in days]


# qlist distribution algorithm #1 - chunks
# each processor is given a continous block of qlist items to process

# if len(qlist) > size:

# 	jobs = len(qlist) # num of jobs to run
# 	even = jobs // size # min jobs each processor will run
# 	left = jobs % size # num of jobs to be split between some size

# 	# for each node assign jobs

# 	if rank < left:
# 		r = range( rank*even+rank, (rank+1)*even+rank+1 )
# 	else:
# 		r = range( rank*even+left, (rank+1)*even+left )


# 	for i in r:

# 		# print "("+str(rank)+") " + qlist[i] + "\n"

# 		cmd = runscript+" "+qlist[i][0]+" "+qlist[i][1]
# 		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)

# 		print sts

# elif len(qlist) > rank:

# 	# print "("+str(rank)+") " + qlist[rank] + "\n"

# 	cmd = runscript+" "+qlist[rank][0]+" "+qlist[rank][1]
# 	sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)

# 	print sts



# qlist distribution algorithm #1 - cyclic
# qlist items are distributed to processors in a cyclical manner

c = rank
while c < len(qlist):

	try:
		cmd = runscript+" "+qlist[c][0]+" "+qlist[c][1]
		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
		print sts

	except sp.CalledProcessError as sts_err:                                                                                                   
	    print "subprocess error code", sts_err.returncode, sts_err.output

	c += size


comm.Barrier()
