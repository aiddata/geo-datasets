
from mpi4py import MPI

import subprocess as sp


runscript = './mosaic_hansen.sh'


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


# qlist = [
#     'https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/treecover2000.txt',
#     'https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/loss.txt',
#     'https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/gain.txt',
#     'https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/lossyear.txt',
#     'https://storage.googleapis.com/earthenginepartners-hansen/GFC2015/datamask.txt'
# ]

qlist = [
    'treecover2000',
    'loss',
    'gain',
    'lossyear',
    'datamask'
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
