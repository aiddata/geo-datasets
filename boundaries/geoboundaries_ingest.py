"""
Example usage:

python geo-datasets/boundaries/geoboundaries_ingest.py master 1_3_3 serial missing True

Where args are: branch, version, method, update mode, dry run

Note: when using parallel mode, be sure to spin up job first (manually or use job script)
      and use appropriate mpi command to run script

qsub -I -l nodes=2:c18c:ppn=16 -l walltime=48:00:00
mpirun --mca mpi_warn_on_fork 0 --map-by node -np 32 python-mpi /path/to/geo-datasets/boundaries/geoboundaries_ingest.py master 1_3_3 parallel missing True

"""

import sys
import os


main_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))), 'geo-hpc')

sys.path.insert(0, os.path.join(main_dir, 'utils'))
sys.path.insert(0, os.path.join(main_dir, 'ingest'))


import mpi_utility
from config_utility import BranchConfig
import add_geoboundaries as add_gb


branch = sys.argv[1]

config = BranchConfig(branch=branch)

# check mongodb connection
if config.connection_status != 0:
    raise Exception("connection status error: {0}".format(
        config.connection_error))


# -------------------------------------------------------------------------


import time

# format: 1_3_3
version = sys.argv[2]

data_dir = os.path.join(config.data_root, 'geo/data/boundaries/geoboundaries', version)


if not os.path.isdir(data_dir):
    msg = 'Could not find directory for GeoBoundaries version ({0})'.format(
        version)
    raise Exception(msg)


method = sys.argv[3]


if len(sys.argv) >= 5:
    update = sys.argv[4]
else:
    update = False

if len(sys.argv) >= 6:
    dry_run = sys.argv[5]
else:
    dry_run = False


qlist = [os.path.join(data_dir, i) for i in os.listdir(data_dir)
         if os.path.isdir(os.path.join(data_dir, i))]
qlist.sort()


job = mpi_utility.NewParallel(parallel=method)


if job.rank == 0:
    print 'GeoBoundaries found: {}'.format(len(qlist))



def tmp_master_init(self):
    # start job timer
    self.Ts = int(time.time())
    self.T_start = time.localtime()
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)


def tmp_master_process(self, worker_data):
    pass


def tmp_master_final(self):

    # stop job timer
    T_run = int(time.time() - self.Ts)
    T_end = time.localtime()
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', self.T_start)
    print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
    print 'Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'
    print '\n\n'


def tmp_worker_job(self, task_index, task_data):

    path = task_data

    try:
        with mpi_utility.Capturing() as output:
            add_gb.run(path=path, version=version, config=config,
                       update=update, dry_run=dry_run)
        print '\n'.join(output)
    except:
        print "Error with {0}".format(path)
        raise

    return 0


# init / run job
job.set_task_list(qlist)

job.set_master_init(tmp_master_init)
job.set_master_process(tmp_master_process)
job.set_master_final(tmp_master_final)
job.set_worker_job(tmp_worker_job)

job.run()


