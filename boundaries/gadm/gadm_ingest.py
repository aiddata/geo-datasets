"""
Example usage:

python geo-datasets/boundaries/gadm/gadm_ingest.py master 2.8 serial missing True

Where args are: branch, version, method, update mode, dry run

Note: when using parallel mode, be sure to spin up job first (manually or use job script)
      and use appropriate mpi command to run script

qsub -I -l nodes=2:c18c:ppn=16 -l walltime=48:00:00
mpirun --mca mpi_warn_on_fork 0 --map-by node -np 32 python-mpi /path/to/geo-datasets/boundaries/gadm/gadm_ingest.py master 2.8 parallel missing True

"""

import sys
import os


main_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))), 'geo-hpc')

sys.path.insert(0, os.path.join(main_dir, 'utils'))
sys.path.insert(0, os.path.join(main_dir, 'ingest'))


from config_utility import BranchConfig
import add_gadm


branch = sys.argv[1]

config = BranchConfig(branch=branch)

# check mongodb connection
if config.connection_status != 0:
    raise Exception("connection status error: {0}".format(
        config.connection_error))


# -------------------------------------------------------------------------


import time


version = sys.argv[2]

version_dir = 'gadm{0}'.format(version)

data_dir = os.path.join(config.data_root, 'geo/data/boundaries', version_dir)


if not os.path.isdir(data_dir):
    msg = 'Could not find download directory for GADM version ({0})'.format(
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


###
# active_iso3_list = config.release_gadm.values() + config.other_gadm
# print "Active iso3 list: {0}".format(active_iso3_list)

# print qlist
# qlist = [i for i in qlist if os.path.basename(i)[:3] in active_iso3_list]
# print qlist
###


if method == "serial":

    Ts = int(time.time())
    T_start = time.localtime()
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start)

    for path in qlist:
        add_gadm.run(path=path, config=config,
                     update=update, dry_run=dry_run)

    T_run = int(time.time() - Ts)
    T_end = time.localtime()
    print '\n\n'
    print 'Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start)
    print 'End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
    print 'Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'
    print '\n\n'


elif method == "parallel":

    import mpi_utility
    job = mpi_utility.NewParallel()


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


    def tmp_worker_job(self, task):

        task_index, task_data = task
        path = task_data

        try:
            with mpi_utility.Capturing() as output:
                add_gadm.run(path=path, config=config,
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


else:
    raise Exception("Invalid processing method provided ({0})".format(method))

