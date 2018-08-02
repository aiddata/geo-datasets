

import os
from prepare_monthly import DataAggregation
from datetime import datetime

# mode = "serial"
mode = "parallel"

# NOTE: use `qsub jobscript` for running parallel
if mode == "parallel":
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

build_list = [
    # "daily",
    # "monthly",
    "yearly"
]

filter_options = {
    'use_sensor_accept': False,
    'sensor_accept': [],
    'use_sensor_deny': False,
    'sensor_deny': [],
    'use_year_accept': False,
    'year_accept': ['1987'],
    'use_year_deny': True,
    'year_deny': ['2017']
}

src_base = r"/sciclone/aiddata10/REU/geo/data/rasters/cru_ts4.01/monthly/pre"
dst_base = r"/sciclone/aiddata10/REU/pre_geo/CRU/precipitation/yearly"
dataname = "cru_precipitation"

builder = DataAggregation(src_base = src_base,
                          dst_base= dst_base,
                          filter_options=filter_options,
                          prefix=dataname)


# -----------------------------------------------------------------------------

print "generating initial data list..."

ref = builder.build_data_list()

# -------------------------------------

print "building year list..."

year_months = {}

monthlist = [i for i in os.listdir(src_base) if i.endswith('.tif')]

for dmonth in monthlist:

    dyear = dmonth.split(".")[2]

    if year_months.has_key(dyear):

        year_months[dyear].append(os.path.join(src_base, dmonth))

    else:

        year_months[dyear] = list()
        year_months[dyear].append(os.path.join(src_base, dmonth))


year_qlist = [(year, month_path) for year, month_path in year_months.iteritems()]



# -------------------------------------


print "running yearly data..."

if "yearly" in build_list:

    if mode == "parallel":

        c = rank
        while c < len(year_qlist):

            try:
                builder.prep_yearly_data(year_qlist[c])
            except Exception as e:
                print "Error processing year: {0}".format(year_qlist[c][0])
                # raise
                print e
                # raise Exception('year processing')


            c += size

        comm.Barrier()

    elif mode == "serial":

        for c in range(len(year_qlist)):
            builder.prep_yearly_data(year_qlist[c])

    else:
        raise Exception("Invalid `mode` value for script.")


