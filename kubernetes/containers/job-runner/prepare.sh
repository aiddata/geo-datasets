#!/bin/bash

# print all executed commands
set -x

git clone -b with-package https://github.com/jacobwhall/geodata-container.git --depth 1
pip install ./geodata-container/containers/geo_datasets

# run args
exec "$@"
