
library(gdalUtils)
library(lubridate)

#setwd("/Users/miranda/Documents/AidData/projects/datasets/MODIS_temp/output")
#indir = "/Users/miranda/Documents/AidData/projects/datasets/MODIS_temp/rawdata"

setwd("/sciclone/aiddata10/REU/pre_geo/modis_temp/temp")
indir = "/sciclone/aiddata10/REU/pre_geo/modis_temp/rawdata"

# get the month of the hdf data file
get_time <- function(yearnum, datenum){
  filetime <- strptime(paste(yearnum, datenum), format="%Y %j")
  format.Date(filetime,"%m")
}

# get a date stamp which will be used in the export data file name
get_tstamp <- function(filename){
  
  datestring <- strsplit(basename(filename),split=".",fixed=TRUE)[[1]][2]
  
  if (nchar(datestring) == 8){
    
    yeartime <- substr(datestring,2,5)
    daytime <- substr(datestring,6,8)
    mth <- get_time(yeartime, daytime)
    paste(yeartime, mth, sep="")
    
  } else{
    
    warning('wrong time')
    
  }
  
}

rlist <- list.files(indir, pattern="hdf$", full.names=FALSE)
for (f in rlist){
  
  try(sds <- get_subdatasets(file.path(indir,f)))
  
  name1 <- sds[1]
  name2 <- sds[6]
  
  tstamp <- get_tstamp(name1)
  
  # filename: modis_lst_day_cmg_yyyymm.tif
  # filename: modis_lst_night_cmg_yyyymm.tif
  newfile1 <- paste0("modis_lst_day_cmg", tstamp, ".tif")
  newfile2 <- paste0("modis_lst_night_cmg", tstamp, ".tif")
  gdal_translate(name1, dst_dataset = newfile1)
  gdal_translate(name2, dst_dataset = newfile2)
  
}








