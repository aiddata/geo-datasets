#!/bin/bash

# print all executed commands
set -x
set -euo pipefail

if [[ -z "${DATA_MANAGER_VERSION:-}" ]]; then
	echo "DATA_MANAGER_VERSION environment variable is not set! Exiting..."
	exit 1
elif [ "$DATA_MANAGER_VERSION" = "latest" ] ; then
	branch=master
else
	branch=data_manager_$DATA_MANAGER_VERSION
fi

# The image already ships data_manager, but the pinned version is installed over
# it here so a dataset can move to a new release without rebuilding the image.
# Flow pods run as an arbitrary non-root uid, so both the checkout and the
# install have to land somewhere writable rather than in /opt or system
# site-packages. User site-packages precedes the system one on sys.path, so this
# shadows the baked-in copy.
workdir=$(mktemp -d)
git clone --branch "$branch" --depth 1 https://github.com/aiddata/geo-datasets.git "$workdir/geo-datasets" \
	|| { echo "ERROR: Failed to download data_manager package. Does the tag $branch exist in the git repository? Exiting..."; exit 1; }
pip install --user --no-cache-dir "$workdir/geo-datasets/data_manager"
rm -rf "$workdir"

# run args
exec "$@"
