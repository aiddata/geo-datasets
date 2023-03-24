#!/bin/bash

# print all executed commands
set -x

git clone --branch data_manager_$DATA_MANAGER_VERSION https://github.com/aiddata/geo-datasets.git --depth 1
/opt/conda/bin/pip install ./geo-datasets/global_scripts/data_manager

# run args
exec "$@"
