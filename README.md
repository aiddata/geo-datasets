# geo-datasets


## Overview:

each dataset directory should contain

- a readme detailing **all** the steps needed to reproduce from scratch. This may include a download script or instructions on how to manually download, instructions for running any processing scripts and/or step by step instructions for any manual processing, as well as any other relevant information about the dataset and processing (caveats, notes, suggested usage, etc.)
- all scripts used to produce data
- ingest json prepared according to standard format used by geo framework (see existing datasets for examples)


## Preparing data on SciClone:

all raw data should be initially downloaded into the `/sciclone/aiddata10/REU/pre_geo/raw` directory and processing should output to the `/sciclone/aiddata10/REU/pre_geo/data` directory

after code review, testing, quality assurance steps data will be moved the `raw` and `data` directories within `/sciclone/aiddata10/REU/pre_geo` to their respective directories in `/sciclone/aiddata10/REU/geo` for ingest into the geo framework
