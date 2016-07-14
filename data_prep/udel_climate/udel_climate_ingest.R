

library(sp)
library(raster)


args <- commandArgs(trailingOnly = TRUE)

dataset <- args[1]
# dataset <- 'precip_2014'
# dataset <- 'air_temp_2014'

# base_dir <- '/sciclone/aiddata10/REU'
# raw_dir <- sprintf('%s/raw/%s', base_dir, dataset)
# data_dir <- sprintf('%s/data/rasters/external/global/udel_%s',
#                     base_dir, dataset)

base_dir <- '/home/userz/Desktop'
raw_dir <- sprintf('%s/udel_test/%s', base_dir, dataset)
data_dir <- sprintf('%s/udel_test/udel_%s',
                    base_dir, dataset)


if (!dir.exists(raw_dir)) {
  msg <- sprintf("directory not found (%s)", raw_dir)
  stop(msg)
}

flist <- list.files(raw_dir)

if (length(flist) == 0) {
  msg <- sprintf("no files found (%s)", raw_dir)
  stop(msg)
}


dir.create(data_dir)

for (fname in flist) {

  fpath <- sprintf('%s/%s', raw_dir, fname)
  data <- read.table(fpath)
  names(data) <- c("lon", "lat",
                   "1", "2", "3", "4", "5", "6",
                   "7", "8", "9", "10", "11", "12")

  coordinates(data) = ~lon+lat
  proj4string(data) = CRS("+init=epsg:4326")

  for (m in 1:12) {
    data_trim <- data[, m]
    gridded(data_trim) = TRUE
    r <- raster(data_trim)

    out_name <- sprintf('%s_%s.tif', gsub("[.]", "_", fname), m)
    out_path <- sprintf('%s/%s', data_dir, out_name)
    writeRaster(r, file=out_path, overwrite=TRUE)
  }

}

warnings()

