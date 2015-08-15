#############################################
#===========================================#
# --- Putting the whole system together --- #
#===========================================#
#############################################

#=====================#
# - Necessary Setup - #
#=====================#

# - Environment Variables - #
path <- "C:/Users/Nitin/Desktop/MIDS/DS210/2.5_months_BART"
setwd(path)

# - Loading R Scripts - #
source("lag_time_model.R")
source("data_seq.R") #lags here for a couple of seconds
source("sequence_matching.R")

# - Lookup Table - #
stations <- read.csv("station_lookup_table.csv")

#=======================#
# - Automated Process - #
#=======================#
# - Putting it all together - #
#This function will do the following:
#Given an input sequence, we will compute the next arrival time
#From this, we will add on the predicted value from the lag_times function output
#Then, we will treat this as our new sequence
#We will continue in this manner until the desired location has been reached

# - User Inputs - #
#start_loc #some string here
#end_loc #some other location here
#desired_depart_time #some string here to coerce to a real number
#desired_arrival_time #some other string here to coerce to a real number
#depart_am #some string here

#example for concreteness
start_loc <- "WOAK"
end_loc <- "POWL"
desired_depart_time <- "7:15"
depart_am <- "AM"

loc_list <- as.character(stations$station_name)
index_start_loc <- which(loc_list %in% start_loc)
index_end_loc <- which(loc_list %in% end_loc)

start_loc2 <- as.character(stations$r_name[as.character(stations$station_name) == as.character(start_loc)])
end_loc2 <- as.character(stations$r_name[as.character(stations$station_name) == as.character(end_loc)])
desired_depart_time2 <- time_stamp_fixer(desired_depart_time)
#desired_arrival_time <- time_stamp_fixer(desired_arrival_time)
depart_am2 <- as.logical(depart_am=="AM") #turn into a 1 if am, otherwise coerce to 0
locs <- c(start_loc2, end_loc2)

current_seq <- c(6.24, 6.34,6.44,7.01) #to be constructed from sql query

pred_output_name <- c()
pred_output_time <- c()

while (index_start_loc <= index_end_loc) {
  #do sequence matching to get the arrival time to the starting location
  arr_pred <- next_seq_term(current_seq, 10, train_set_times, train_set_between)
  #next, find the full lag time at that given station
  pre_array <- cbind(reg_coef,c(1, rep(0,36)))
  names(pre_array)[4] <- "include"
  pre_array$include[pre_array$features %in% locs] <- 1
  pre_array$include[is.element(as.character(pre_array$features),"am")] <- depart_am2 
  pre_array$include[is.element(as.character(pre_array$features),"arrival_time")] <- desired_depart_time2
  lag_pred <- sum(pre_array$include*pre_array$coefficients)
  #compute departure time
  depart_pred <- arr_pred + lag_pred
  pred_output_name <- c(pred_output_name, as.character(loc_list[index_start_loc]))
  pred_output_time <- c(pred_output_time, depart_pred)
  #print value for now - will pass to object later
  print(depart_pred)
  #update with new info to continue predictions
  current_seq <- c(current_seq,depart_pred)
  index_start_loc <- index_start_loc + 1 
}

pred_output <- data.frame(pred_output_name, sapply(pred_output_time, reverse_time_stamp))
names(pred_output) <- c("Station", "Predicted Departure Time")
print(pred_output)

#===============#
# - Next Step - #
#===============#
#Be able to query db to find the current trajectory of the train

library(RMySQL)
mydb <- dbConnect(MySQL(), user='sl_user', password='sl_password', dbname='bart_project', host='158.85.198.80')
#dbDisconnect(mydb)

#Based on the user preference, we need to execute the proper query
#Here's the string that we will use to build up the rest of the queries
#"station" will be an input based on what the user chooses as their departure location

#will need to transform the station name from the ui to the version that the bart api uses
#this can be done using the stations table (loaded from csv above)

#station_name <- as.character(stations$r_name[as.character(stations$station_name) == start_loc])
station_name <- start_loc

date <- Sys.Date()
#need to transform R's system date to SQL's format
year <- strsplit(as.character(date), "-")[[1]][1]
month <- strsplit(as.character(date), "-")[[1]][2]
day <- strsplit(as.character(date), "-")[[1]][3]
new_date <- paste0(month,"/",day,"/",year)

#Need to query for all possible stops before the station in question

query <- paste0("SELECT * FROM rt_etd WHERE minutes = 'Leaving' and abbr = '",station_name,
                "' and abbreviation = 'PITT' and direction = 'North' and date = '",new_date,"' limit 10")

execute_query <- dbGetQuery(mydb, query)
