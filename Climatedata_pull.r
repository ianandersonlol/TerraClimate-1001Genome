arabidopsis_data <- read.csv("/Users/iananderson/Desktop/TerraClimate-1001Genome/availableplantsforclimate.csv",header = TRUE)

# enter in variable you want to download see: http://thredds.northwestknowledge.net:8080/thredds/terraclimate_aggregated.html
vars <- list("aet","def","pet","ppt","q","soil","srad","swe","tmax","tmin","vap","ws","vpd","PDSI")

#install it if you need it
#install.packages("ncdf4")
library(ncdf4)

for (i in vars) {
  baseurlagg <- paste0(paste0("http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_terraclimate_",i),"_1958_CurrentYear_GLOBE.nc")
  nc <- nc_open(baseurlagg)
  # if you put opening the thing in the loop every time it takes forever forever. 
  
  export_data <- data.frame(matrix(ncol = 2, nrow = 0))
  colnames(export_data) <- c("Line",i)
  for (j in 1:nrow(arabidopsis_data)) {
    lon <- ncvar_get(nc, "lon")
    lat <- ncvar_get(nc, "lat")
    flat = match(abs(lat - arabidopsis_data[j,3]) < 1/48, 1)
    latindex = which(flat %in% 1)
    flon = match(abs(lon - arabidopsis_data[j,4]) < 1/48, 1)
    lonindex = which(flon %in% 1)
    start <- c(lonindex, latindex, 1)
    count <- c(1, 1, -1)
    
    # read in the full period of record using aggregated files
    
    data <- as.numeric(ncvar_get(nc, varid = i,start = start, count))
    print(paste0(paste0(paste0("Starting on ",arabidopsis_data[j,2])," in "),i))
    for (k in data) {
    export_data[nrow(export_data)+1,] <- c(arabidopsis_data[j,2],k)
      
    }
    
  }
  write.csv(export_data,paste0(paste0("/Users/iananderson/Desktop/TerraClimate-1001Genome/arabidopsis_",i),"_data.csv"), row.names = FALSE)
  print(paste0('writing '),paste0(paste0("/Users/iananderson/Desktop/TerraClimate-1001Genome/arabidopsis_",i),"_data.csv"))
}