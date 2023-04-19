#!/bin/bash

# print all executed commands
set -x

if [ $DATA_MANAGER_VERSION = "latest" ] ; then
	branch=develop-k8s
else
	branch=data_manager_$DATA_MANAGER_VERSION
fi

git clone --branch $branch https://github.com/aiddata/geo-datasets.git --depth 1
pip install ./geo-datasets/global_scripts/data_manager

# run args
exec "$@"
