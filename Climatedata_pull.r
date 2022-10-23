arabidopsis_data <- read.csv("C:\\Users\\ian03\\Desktop\\TerraClimate-1001Genome\\availableplantsforclimate.csv",header = TRUE)

# enter in variable you want to download see: http://thredds.northwestknowledge.net:8080/thredds/terraclimate_aggregated.html
vars <- list("aet","def","pet","ppt","q","soil","srad","swe","tmax","tmin","vap","ws","vpd","PDSI")

#install it if you need it
#install.packages("ncdf4")
#install.packages("zoo")
library('ncdf4')
library('zoo')

for (i in vars) {
  baseurlagg <- paste0(paste0("http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_terraclimate_",i),"_1958_CurrentYear_GLOBE.nc")
  nc <- nc_open(baseurlagg)
  # if you put opening the thing in the Jz loop every time it takes forever forever. 
  
  export_data <- data.frame(matrix(ncol = 4, nrow = 0))
  
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
    print(paste0(paste0(paste0("Starting on ",arabidopsis_data[j,2])," in "),i))
    
    print("loading data... this takes forever")
    data <- as.numeric(ncvar_get(nc, varid = i,start = start, count))
   
    
    print("Converting data to df")
    export_data2 <- as.data.frame(data,colnames=FALSE)
    print("adding line names")
    line_names<- rep(c(arabidopsis_data[j,2]),times=nrow(export_data2))
    line_names<- data.frame(line_names)
    print("Combining lines")
    names_and_data <- cbind(line_names,export_data2)
    print("Adding month/year")
    names_and_data$month <- head(as.yearmon("Jan 1958") + c(0, seq_len(nrow(names_and_data)))/12, -1)
    print("splitting...")
    export_split_dates <- data.frame(do.call('rbind', strsplit(as.character(names_and_data$month),' ',fixed=TRUE)))
    print("combining months to everything...")
    names_and_data <- cbind(names_and_data,export_split_dates)
    names_and_data$month <- NULL
    
    print("combining with mother")
    export_data <- rbind(export_data,names_and_data)
    
    
  }
  colnames(export_data) <- c("Line",i,"Month","Year")
  write.csv(export_data,paste0(paste0("C:\\Users\\ian03\\Desktop\\TerraClimate-1001Genome\\arabidopsis_",i),"_data.csv"), row.names = FALSE)
  #print(paste0('writing '),paste0(paste0("C:\\Users\\ian03\\Desktop\\TerraClimate-1001Genome\\TerraClimate-1001Genome\\arabidopsis_",i),"_data.csv"))
}