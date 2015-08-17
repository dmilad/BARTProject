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

#Create a data frame for all possible stations
MLBR <- data[data$station=="MLBR",]
SFIA <- data[data$station=="SFIA",]
SBRN <- data[data$station=="SBRN",]
BALB <- data[data$station=="BALB",]
GLEN <- data[data$station=="GLEN",]
S_24 <- data[data$station=="24TH",]
S_16 <- data[data$station=="16TH",]
CIVC <- data[data$station=="CIVC",]
POWL <- data[data$station=="POWL",]
MONT <- data[data$station=="MONT",]
EMBR <- data[data$station=="EMBR",]
WOAK <- data[data$station=="WOAK",]
S_12 <- data[data$station=="12TH",]
S_19 <- data[data$station=="19TH",]
MCAR <- data[data$station=="MCAR",]
ROCK <- data[data$station=="ROCK",]
ORIN <- data[data$station=="ORIN",]
LAYF <- data[data$station=="LAFY",]
WCRK <- data[data$station=="WCRK",]
PHIL <- data[data$station=="PHIL",]
CONC <- data[data$station=="CONC",]
NCON <- data[data$station=="NCON",]

#Impose the following rules:
#Rule 1 - if the train length has changed, we have a new train
#Rule 2 - if the train length has not changed in the past 4 minutes, assume a new train
threshold <- 4/60 #4 minute buffer
threshold_mlbr <- 10/60 #since this is the end of the line, needs a bigger buffer

#testing this shit out
EMBR$train_id <- 0
ticker <- 1
for (k in 1:(nrow(EMBR)-1)) {
  if (as.numeric(as.character(EMBR$train_length[k]))!= as.numeric(as.character(EMBR$train_length[k+1]))) {
    EMBR$train_id[k] <- ticker
    ticker <- ticker +1
    EMBR$train_id[k+1] <- ticker
  } else if (abs(as.numeric(as.character(EMBR$time[k]))-as.numeric(as.character(EMBR$time[k+1])))>threshold) {
    EMBR$train_id[k] <- ticker
    ticker <- ticker +1
    EMBR$train_id[k+1] <- ticker    
  } else {
    EMBR$train_id[k] <- ticker
  }
  print(k)
}

WOAK$train_id <- 0
ticker <- 1
for (k in 1:(nrow(WOAK)-1)) {
  if (as.numeric(as.character(WOAK$train_length[k]))!= as.numeric(as.character(WOAK$train_length[k+1]))) {
    WOAK$train_id[k] <- ticker
    ticker <- ticker +1
    WOAK$train_id[k+1] <- ticker
  } else if (abs(as.numeric(as.character(WOAK$time[k]))-as.numeric(as.character(WOAK$time[k+1])))>threshold) {
    WOAK$train_id[k] <- ticker
    ticker <- ticker +1
    WOAK$train_id[k+1] <- ticker    
  } else {
    WOAK$train_id[k] <- ticker
  }
  print(k)
}
#do these match? 
max(WOAK$train_id)
max(EMBR$train_id)
#not exactly, but pretty damn close

train_tagger <- function(df, threshold) {
  df$train_id <- 1
  ticker <- 1
  for (k in 1:(nrow(df)-1)) {
    if (as.numeric(as.character(df$train_length[k]))!= as.numeric(as.character(df$train_length[k+1]))) {
      df$train_id[k] <- ticker
      ticker <- ticker +1
      df$train_id[k+1] <- ticker
    } else if (abs(as.numeric(as.character(df$time[k]))-as.numeric(as.character(df$time[k+1])))>threshold) {
      df$train_id[k] <- ticker
      ticker <- ticker +1
      df$train_id[k+1] <- ticker    
    } else {
      df$train_id[k] <- ticker
    }
    print(k)
  }
  return(df)
}

df_container <- list(MLBR , SFIA , SBRN ,BALB ,GLEN , S_24 , S_16 , CIVC , POWL , MONT , EMBR ,
                     WOAK , S_12 , S_19 , MCAR , ROCK , ORIN , LAYF , WCRK , PHIL , CONC , NCON )
                     

for (j in 1:length(df_container)) {
  df_container[[j]] <- train_tagger(df_container[[j]], threshold)
  print(j)
}

#with the exception of the MLBR station, they all seem to be witin a ball park of one another
for (k in 1:length(df_container)) {
  print(max(df_container[[k]]$train_id))
}