

library(sp)
library(raster)


args <- commandArgs(trailingOnly = TRUE)

dataset <- args[1]
# dataset <- 'precip_2014_v4.01'
# dataset <- 'air_temp_2014_v4.01'

base_dir <- '/sciclone/aiddata10/REU'
raw_dir <- sprintf('%s/raw/udel_climate/%s', base_dir, dataset)
data_dir <- sprintf('%s/data/rasters/external/global/udel_climate/%s',
                    base_dir, dataset)

# base_dir <- '/home/userz/Desktop'
# raw_dir <- sprintf('%s/udel_test/%s', base_dir, dataset)
# data_dir <- sprintf('%s/udel_test/udel_%s',
#                     base_dir, dataset)


if (!file.exists(raw_dir)) {
  msg <- sprintf("directory not found (%s)", raw_dir)
  stop(msg)
}

flist <- list.files(raw_dir)

if (length(flist) == 0) {
  msg <- sprintf("no files found (%s)", raw_dir)
  stop(msg)
}


dir.create(sprintf('%s/monthly', data_dir), recursive=TRUE)
dir.create(sprintf('%s/yearly/mean', data_dir), recursive=TRUE)
dir.create(sprintf('%s/yearly/min', data_dir), recursive=TRUE)
dir.create(sprintf('%s/yearly/max', data_dir), recursive=TRUE)


for (fname in flist) {

  fpath <- sprintf('%s/%s', raw_dir, fname)
  data <- read.table(fpath)
  months <- as.character(c(1:12))
  names(data) <- c("lon", "lat", months)

  coordinates(data) = ~lon+lat
  proj4string(data) = CRS("+init=epsg:4326")


  # monthly
  for (m in months) {
    data_trim <- data[, m]
    gridded(data_trim) = TRUE
    r <- raster(data_trim)

    out_name <- sprintf('%s_%s.tif', gsub("[.]", "_", fname), m)
    out_path <- sprintf('%s/monthly/%s', data_dir, out_name)
    writeRaster(r, file=out_path, overwrite=TRUE)
  }


  # yearly
  methods <- c('mean', 'min', 'max')
  for (j in methods) {
    data[[j]] <- apply(data@data[,as.character(c(1:12))], 1, j)
    data_trim <- data[, j]
    gridded(data_trim) = TRUE
    r <- raster(data_trim)

    out_name <- sprintf('%s_%s.tif', gsub("[.]", "_", fname), j)
    out_path <- sprintf('%s/yearly/%s/%s', data_dir, j, out_name)
    writeRaster(r, file=out_path, overwrite=TRUE)
  }


}

warnings()

