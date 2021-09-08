
import sys
import os
# import datetime
# import json
# from warnings import warn
# from pprint import pprint
# from unidecode import unidecode

utils_dir = "/sciclone/aiddata10/geo/master/source/geo-hpc/utils"
# utils_dir = os.path.join(
#     os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

import ingest_resources as ru
from ingest_database import MongoUpdate

from add_geoboundaries import run


branch = "master"

from config_utility import BranchConfig

config = BranchConfig(branch=branch)

# check mongodb connection
if config.connection_status != 0:
    raise Exception("connection status error: {0}".format(
        config.connection_error))


# -------------------------------------


path = "/sciclone/aiddata10/REU/geo/data/boundaries/geoboundaries/v4/JAM_ADM1"

version = os.path.basename(os.path.dirname(path))

generator = "manual"

update = "full"

dry_run = False

run(path=path, version=version, config=config, generator=generator,
    update=update, dry_run=dry_run)
