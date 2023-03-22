#!/bin/bash

# print all executed commands
set -x

git clone -b develop-k8s https://github.com/jacobwhall/geo-datasets.git --depth 1
pip install ./geo-datasets/global_scripts/geo_datasets

# run args
exec "$@"
