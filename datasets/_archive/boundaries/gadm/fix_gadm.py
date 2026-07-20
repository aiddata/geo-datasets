# update gadm after gadm_ingest.sh and add_gadm.py have already run
#
# 2016-04-12: changes made via this script have been added to add_gadm.py
#             so this script will not be needed in future. Keeping it to
#             to serve as template / tool in case other changes need to made.

# -----------------------------------------------------------------------------

import sys
import os

branch = sys.argv[1]

branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

if not os.path.isdir(branch_dir):
    raise Exception('Branch directory does not exist')


config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
sys.path.insert(0, config_dir)

from config_utility import *

config = BranchConfig(branch=branch)

# -------------------------------------

# check mongodb connection
if config.connection_status != 0:
    sys.exit("connection status error: " + str(config.connection_error))

# -----------------------------------------------------------------------------


import json
import pymongo



# -----------------------------------------------------------------------------


def quit(reason):

    # do error log stuff
    #

    # output error logs somewhere
    #

    # if auto, move job file to error location
    #

    sys.exit("add_gadm.py: Terminating script - "+str(reason)+"\n")


# init data package
dp = {}

# get release base path
if len(sys.argv) > 2:

    path = sys.argv[2]

    if os.path.isdir(path):
        dp['base'] = path
    else:
        quit("Invalid base directory provided.")

else:
    quit("No base directory provided")


# add version input here
if len(sys.argv) > 3:

    gadm_version = sys.argv[3]

    try:
        gadm_version = float(gadm_version)
    except:
        quit("Invalid GADM version provided.")

else:
    quit("No GADM version provided")



# remove trailing slash from path
if dp["base"].endswith("/"):
    dp["base"] = dp["base"][:-1]



gadm_name = os.path.basename(dp["base"])

gadm_iso3 = gadm_name[:3]
gadm_adm = gadm_name[4:]

gadm_lookup_path = os.path.join(branch_dir, 'asdf', 'src', 'gadm_iso3.json')
gadm_lookup =  json.load(open(gadm_lookup_path, 'r'))

gadm_country = gadm_lookup[gadm_iso3]



dp["name"] = (gadm_iso3.lower() + "_" + gadm_adm.lower() + "_gadm" +
             str(gadm_version).replace('.', ''))


dp["gadm_info"] = {}
dp["gadm_info"]["country"] = gadm_country
dp["gadm_info"]["iso3"] = gadm_iso3
dp["gadm_info"]["adm"] = int(gadm_adm[-1:])



# update mongo
print "\nUpdating..."

# connect to database and asdf collection
client = pymongo.MongoClient(config.server)
asdf = client[config.asdf_db]


# gadm_col_str = "data"
gadm_col_str = "gadm" + str(gadm_version).replace('.', '')

c_data = asdf[gadm_col_str]

c_data.update_one({'name': dp['name']}, {'$set': {'gadm_info': dp["gadm_info"]}})
