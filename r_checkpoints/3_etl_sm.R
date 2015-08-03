#===========================#
# - Sequence Matching ETL - #
#===========================#

#===============#
# - Workspace - #
#===============#

path <- "C:/Users/Nitin/Desktop/MIDS/DS210/2.5_months_BART"
setwd(path)

data <- read.csv("lag_times_ROCK2.csv")
View(head(data))

#===============#
# - ETL Magic - #
#===============#

#get list of the stations
stations <- unique(as.character(data$station))

#match up the stations in a north-bound order 
station_list <- c('PITT', 'NCON', 'CONC', 'PHIL', 'WCRK', 'LAFY', 'ORIN', 'ROCK', 'MCAR', '19TH', '12TH',
                  'WOAK', 'EMBR', 'MONT', 'POWL', 'CIVC', '16TH', '24TH', 'GLEN', 'BALB',
                  'SBRN', 'SFIA', 'MLBR') 

#figure out which extra ones we have
catcher <- c()
for (k in 1:length(station_list)) {
  if (is.element(as.character(station_list[k]),stations)) {
    catcher <- c(catcher,1)
  } else {
    catcher <- c(catcher,0)
  }
  print(k)
} 
print(station_list[!catcher]) 

#we are missing PITT, but this makes sense since the train is now leaving SOUTHBOUND after it reaches PITT
#------------------------------------------#
library(RMySQL)

mydb <- dbConnect(MySQL(), user='sl_user', password='sl_password', dbname='bart_project', host='158.85.198.80')

query <- "SELECT * FROM rt_etd WHERE minutes = 'Leaving' and abbreviation = 'PITT'
                     and direction = 'South' limit 1000000"
pitt_data <- dbGetQuery(mydb, query)
dbDisconnect(mydb)

write.csv(pitt_data, "PITT.csv")

#------------------------------------------#

#construction the rest of the table without PITT right now (until the query gets fixed)

station_list <- rev(station_list) #reverse the station_list to get it going in the correct direction

arrivals <- as.data.frame(t(as.matrix(rep(0, length(station_list)))))
arrivals <- cbind(arrivals, 0, 0) 
#this second to last column will hold train_length information
#the last column will hold the am_pm stamp of the MILB line when the path started

colnames(arrivals) <- c(station_list, "train_length", "MLBR_marker")

data[data$station=="MLBR",]
