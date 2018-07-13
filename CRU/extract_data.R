library(cruts)
library(raster)
library(rgdal)

setwd("/Users/miranda/Documents/AidData/projects/datasets/CRU/output")
outdir <- "/Users/miranda/Documents/AidData/projects/datasets/CRU/output"
# get input .nc to a list in file.names
path = "/Users/miranda/Documents/AidData/projects/datasets/CRU/CRU_RawData"
out.file<-""
file.names <- dir(path, pattern =".nc")

# make output folder
vars <- c("cld", "dtr", "frs", "pet", "pre", "rhm", "ssh", "tmp", "tmn", "tmx", "vap", "wet", "wnd");

for (var in vars){
  # change working directory back to output
  setwd("/Users/miranda/Documents/AidData/projects/datasets/CRU/output");
  dir.create(var);
  for (file in file.names){
    if (grepl(var, file)){
      # change directory to the output varaible folder
      setwd(file.path("/Users/miranda/Documents/AidData/projects/datasets/CRU/output",var));
      filepath <- file.path(path,file);
      rstlayers <- cruts2raster(filepath, 
                                timeRange=c("1901-01-01","2017-01-01"));
      dtype <- dataType(rstlyr)
      for (lyr in 1:nlayers(rstlayers)){
        rstlyr <- raster(rstlayers, layer=as.integer(lyr));
        lyr_name = paste("cru",var,substr(names(rstlyr),2,8), "tif",sep=".");
        writeRaster(rstlyr, filename=lyr_name, overwrite=TRUE, format="GTiff",options=c("COMPRESS=NONE"), NAflag=-1, datatype=dtype);
      
        }
    }
  }
}

