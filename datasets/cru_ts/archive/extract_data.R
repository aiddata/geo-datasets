library(cruts)
library(raster)
library(rgdal)


input_dir <- "/sciclone/aiddata10/REU/geo/raw/cru_ts4.01"

output_dir <- "/sciclone/aiddata10/REU/geo/data/rasters/cru_ts4.01/monthly"


# list of var short names
vars <- c("cld", "dtr", "frs", "pet", "pre", "tmp", "tmn", "tmx", "vap", "wet");
# "rhm", "ssh", "wnd"

for (var in vars){

  var_dir <- file.path(output_dir, var)
  dir.create(var_dir, recursive=TRUE);
  setwd(var_dir);

  # gen file name for var
  file.name <- paste("cru_ts4.01.1901.2016.", var, ".dat.nc", sep="")

  # input data path
  filepath <- file.path(input_dir, file.name);

  # generates raster stack from raw input var data
  raster_stack <- cruts2raster(filepath, timeRange=c("1901-01-01","2017-01-01"));

  # output rasters from stack to file
  for (raster_index in 1:nlayers(raster_stack)){

    raster_data <- raster(raster_stack, layer=as.integer(raster_index));
    dtype <- dataType(raster_data)
    raster_temporal <-substr(names(raster_data), 2, 8)
    raster_name <- paste("cru", var, raster_temporal, "tif",sep=".");
    writeRaster(raster_data, filename=raster_name, overwrite=TRUE, format="GTiff", options=c("COMPRESS=NONE"), NAflag=-1, datatype=dtype);

  }


}

