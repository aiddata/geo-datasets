from mpi4py import MPI
import subprocess as sp
import sys
import os

runscript = sys.argv[1]


comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()


def isInt(numStr, numType):
	try:
		i = int(numStr)
		if numType == 'year' and i >= 1981 and i <= 2002:
			return True
		elif numType == 'month' and i >= 01 and i <= 12:
			return True
		else:
			return False
	except:
		return False


# base path historic year_month files are located
path_base = "/sciclone/aiddata10/REU/raw/historic_gimms_ndvi"

# list of all [year, file] combos
qlist = [[name[14:18], name[18:20], name] for name in os.listdir(path_base) if not os.path.isdir(os.path.join(path_base, name)) and name.endswith('.asc') and isInt(name[14:18], 'year') and isInt(name[18:20], 'month')]


c = rank
while c < len(qlist):

	try:
		cmd = runscript+" "+qlist[c][0]+" "+qlist[c][1]+" "+qlist[c][2]
		sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
		print sts

	except sp.CalledProcessError as sts_err:                                                                                                   
	    print "subprocess error code", sts_err.returncode, sts_err.output

	c += size


comm.Barrier()
