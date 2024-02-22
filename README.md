# geo-datasets

Dataset ingest pipelines for GeoQuery.

## Overview

Each directory contains a full ingest pipeline for a dataset. It should contain:
- A README detailing **all** the steps needed to reproduce from scratch. This may include a download script or instructions on how to manually download, instructions for running any processing scripts and/or step by step instructions for any manual processing, as well as any other relevant information about the dataset and processing (caveats, notes, suggested usage, etc.)
- All scripts used to produce the data
- An ingest JSON prepared in the standard format used by geo framework (see existing datasets for examples)

## Preparing Data on SciClone

All raw data should be initially downloaded into the `/sciclone/aiddata10/REU/pre_geo/raw` directory and processing should output to the `/sciclone/aiddata10/REU/pre_geo/data` directory.

After code review, testing, and quality assurance steps data will be moved from the `raw` and `data` directories within `/sciclone/aiddata10/REU/pre_geo` to their respective directories in `/sciclone/aiddata10/REU/geo` for official inclusion in GeoQuery.

## License

This repository is released under the MIT license. Please see LICENSE.md for more information.
