
from mpi4py import MPI

import subprocess as sp


runscript = '/sciclone/aiddata10/geo/master/source/geo-datasets/hansen_2018/mosaic.sh'


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


qlist = [
    'treecover2000',
    'gain',
    'datamask',
    'lossyear'
]

c = rank
while c < len(qlist):

    try:
        cmd = runscript+" "+qlist[c]
        sts = sp.check_output(cmd, stderr=sp.STDOUT, shell=True)
        print sts

    except sp.CalledProcessError as sts_err:
        print "subprocess error code", sts_err.returncode, sts_err.output

    c += size


comm.Barrier()
