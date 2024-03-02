#!/bin/bash

# print all executed commands
set -x

if [[ -z "${DATA_MANAGER_VERSION}" ]]; then
	echo "DATA_MANAGER_VERSION environment variable is not set! Exiting..."
	exit 1
elif [ $DATA_MANAGER_VERSION = "latest" ] ; then
	branch=master
else
	branch=data_manager_$DATA_MANAGER_VERSION
fi

git clone --branch $branch https://github.com/aiddata/geo-datasets.git --depth 1
pip install ./geo-datasets/global_scripts/data_manager

# run args
exec "$@"
