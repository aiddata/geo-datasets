library(cruts)
library(raster)
library(rgdal)

setwd("/Users/miranda/Documents/AidData/projects/datasets/CRU/output")

cru_ncfile <- "/Users/miranda/Documents/AidData/projects/datasets/CRU/cru_ts4.01.1901.2016.pre.dat.nc"
dest <- "/Users/miranda/Documents/AidData/projects/datasets/CRU/output"

rstlayers <- cruts2raster(cru_ncfile, 
                      timeRange=c("1901-01-01","2017-01-01"))


#1901(index: 1) 1+12*0, 2, 3
#1902(index: 2) 1+12*1, 2+12
#1903(index: 3) 1+12*2, ..
#...
# 2016(index: match(2016, years)) 1+12*index-1

#yrs <- seq(1901, 2016, 1)
#mths <- rep(seq(1,12,1), 2016-1901+1) #number of monthly layers
#lyrnms <- paste("cru.pre",yrs,mths,sep=".")


for (lyr in 1:nlayers(rstlayers)){
  rstlyr <- raster(rstlayers, layer=as.integer(lyr));
  lyr_name = paste("cru.pre",substr(names(rstlyr),2,8), "tif",sep=".");
  print(lyr_name);
  writeRaster(rstlyr, filename=lyr_name, overwrite=TRUE, format="GTiff",options=c("PROFILE=BASELINE"), NAflag=-1);
}


