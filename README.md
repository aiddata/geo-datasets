# asdf-datasets
- track potential datasets and acquisition for aiddata spatial data framework
- code for processing datasets
- some ingest triggers (see gadm)

## data prep
scripts for processing data before running extracts

- **ltdr** (local/sciclone)
   preprocessing for raw ltdr ndvi data

- **ndvi_mosaic** (sciclone)
   scripts for creating a job on the Sciclone cluster which preprocesses/mosaics raw contemporary GIMMS NDVI data in parallel

- **historic_ndvi** (sciclone)
   scripts for creating a job on the Sciclone cluster to process raw historic GIMMS NDVI data (1981-2003)

- **atap** (local)
   creates rasters from raw atap data

- **year_mask** (sciclone)
    mask existing yearly datasets using specified dataset and threshold value


