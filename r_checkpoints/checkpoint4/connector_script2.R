#===============#
# - Connector - #
#===============#
path <- "C:/Users/Nitin/Desktop/MIDS/DS210/2.5_months_BART"
setwd(path)


library(RMySQL)

mydb <- dbConnect(MySQL(), user='sl_user', password='sl_password', dbname='bart_project', host='158.85.198.80')

#========================================#
# - EDA on Lag Time at a given station - #
#========================================#
#station list
#station_list = c('DUBL','CAST','BAYF','SANL','COLS','FTVL','LAKE','WOAK',
#             'EMBR','MONT', 'POWL','CIVC', 'PLZA','16TH','24TH','GLEN','BALB' ,'DALY')

station_list <- c('PITT', 'NCON', 'CONC', 'PHIL', 'WCRK', 'LAFY', 'ORIN', 'ROCK', 'MCAR', '19TH', '12TH',
                 'WOAK', 'EMBR', 'MONT', 'POWL', 'CIVC', '16TH', '24TH', 'GLEN', 'BALB',
                 'SBRN', 'SFIA', 'MLBR') 

#Preliminary tables for basic info
#rt_etd <- dbGetQuery(mydb, "SELECT * FROM rt_etd WHERE minutes = 'Leaving' and abbreviation = 'DALY'
#                     and direction = 'North' limit 10")

rt_etd <- dbGetQuery(mydb, "SELECT * FROM rt_etd WHERE minutes = 'Leaving' and abbreviation = 'MLBR'
                     and direction = 'South' limit 10")

rt_etd <- dbGetQuery(mydb, "SELECT * FROM rt_etd WHERE minutes = 'Leaving' and abbreviation = 'MLBR'
                     and direction = 'South' limit 1000000")

#Observation - from here, we can get the arrival time at a station, and the departure time
#Let's do some eda to find out how long the train stalls at a given stop

#rt_etd <- dbGetQuery(mydb, "SELECT * FROM rt_etd WHERE minutes = 'Leaving' and abbreviation = 'DALY'
#                     and direction = 'North' limit 100000")
View(head(rt_etd))

time_stamp_fixer <- function(string) {
  hour <- as.numeric(strsplit(string,":| ")[[1]][1])
  minutes <- as.numeric(strsplit(string,":| ")[[1]][2])/60
  seconds <- as.numeric(strsplit(string,":| ")[[1]][3])/60
  new_time <- hour + minutes + seconds/100
  am_pm <- as.character(strsplit(string,":| ")[[1]][4])
  #new_time <- paste0(as.character(hour),substr(as.character(minutes/60),2,nchar(as.character(minutes/60))))
  return(c(as.numeric(new_time),am_pm))
}

rt_etd$new_time <- 0
rt_etd$am_pm <- ""
for (k in 1:nrow(rt_etd)) {
  rt_etd$new_time[k] <- as.numeric(time_stamp_fixer(rt_etd$time[k])[1])
  rt_etd$am_pm[k] <- as.character(time_stamp_fixer(rt_etd$time[k])[2])
  print(nrow(rt_etd)-k)
}
  
#since we are only considering one line in one direction at a time, we can just split by dates
#and then when the station name changes, we have a new station

station_lag <- list()

for (k in 1:(nrow(rt_etd)-1)) {
  if (k==1) {
    n <- k
  }
  if (k >= n) {
    arrival_location <- as.character(rt_etd$abbr[k])
    n <- k+1
    time_between <- 0
    while (arrival_location == as.character(rt_etd$abbr[n])) {
      n <- n+1
    }
    time_between <- as.numeric(as.character(rt_etd$new_time[n-1])) - as.numeric(as.character(rt_etd$new_time[k])) 
    station_lag[[k]] <- c(arrival_location, time_between, rt_etd$new_time[k], rt_etd$length[k], rt_etd$am_pm[k])
  }
  print(k)
}

station <- c()
lag <- c()
time <- c()
train_length <- c()
am_pm <- c()

for (k in 1:length(station_lag)) {
  if (is.null(station_lag[[k]])!=T) {
    station <- c(station,station_lag[[k]][1])
    lag <- c(lag, station_lag[[k]][2])
    time <- c(time, station_lag[[k]][3])
    train_length <- c(train_length, station_lag[[k]][4])
    am_pm <- c(am_pm, station_lag[[k]][5])
  }
  print(k)
}

lag_times <- data.frame(as.character(station), as.numeric(as.character(lag)), 
                        as.numeric(as.character(time)), as.numeric(as.character(train_length)),
                        as.character(am_pm))
colnames(lag_times) <- c("station", "lag", "time", "train_length", "am_pm")

write.csv(lag_times, "lag_times_ROCK3.csv")

#-#-#-#
View(lag_times)
summary(lag_times)

avg_lags <- list()

for (k in 1:length(station_list)) {
  avg_lags[[k]] <- c(k, mean(as.numeric(as.character(lag_times$lag[lag_times$station==station_list[k]]))))
}

#==================#
# - Other Tables - #
#==================#

weather_current <- dbGetQuery(mydb, "SELECT * FROM weather_current limit 10")
weather_forecast <- dbGetQuery(mydb, "SELECT * FROM weather_forecast limit 10")
sched_load <- dbGetQuery(mydb, "SELECT * FROM sched_load limit 10")
sched_routesched <- dbGetQuery(mydb, "SELECT * FROM sched_routesched limit 10")

#let's construct a table that has all the time stamps for the trains heading North 

#========================#
# - Execute Disconnect - #
#========================#
dbDisconnect(mydb)

