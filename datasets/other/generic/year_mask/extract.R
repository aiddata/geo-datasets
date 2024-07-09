# extract for data with data/year.ext


library("rgdal")
library("raster")
library("maptools")

timer <- proc.time()

readIn <- commandArgs(trailingOnly = TRUE)


# =========================

vector <- readIn[1]
raster <- readIn[2]
output <- readIn[3]

# =========================


myVector <- readShapePoly(vector)

myRaster <- raster(raster) 
# print(summary(myRaster))

myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, weights=TRUE, small=TRUE, na.rm=TRUE)
# myExtract <- extract(myRaster, myVector, fun=mean, sp=TRUE, na.rm=TRUE)
# print(myExtract@data)


colnames(myExtract@data)[length(colnames(myExtract@data))] <- "ad_extract"


dir.create(dirname(output), recursive=TRUE)

write.table(myExtract@data, paste(output, ".csv", sep=""), quote=T, row.names=F, sep=",")

# writePolyShape(myExtract, paste(output,".shp", sep=""))


timer <- proc.time() - timer
print(paste("extract completed in", timer[3], "seconds."))
