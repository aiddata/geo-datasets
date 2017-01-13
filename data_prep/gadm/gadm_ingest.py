
import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'utils')
sys.path.insert(0, config_dir)

from config_utility import BranchConfig

config_attempts = 0
while True:
    config = BranchConfig(branch=branch)
    config_attempts += 1
    if config.connection_status == 0 or config_attempts > 5:
        break

# -------------------------------------------------------------------------


import time


version = sys.argv[2]

version_dir = 'gadm{0}'.format(version)

data_dir = os.path.join(config.data_root, 'data/boundaries', version_dir)


if not os.path.isdir(data_dir):
    msg = 'Could not find download directory for GADM version ({0})'.format(
        version)
    raise Exception(msg)


ingest_dir = os.path.join(branch_dir, 'asdf', 'src', 'ingest')
sys.path.insert(0, ingest_dir)
import add_gadm

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

    sys.path.insert(0, os.path.dirname(ingest_dir))
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

