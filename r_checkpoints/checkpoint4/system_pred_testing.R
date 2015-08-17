#=======================================#
# - Putting the whole system together - #
#=======================================#

# - Environment Variables - #
path <- "C:/Users/Nitin/Desktop/MIDS/DS210/2.5_months_BART"
setwd(path)

# - Loading R Scripts - #
source("lag_time_model.R")
source("data_seq.R") #lags here for a couple of seconds
source("sequence_matching.R")

# - Putting it all together - #
#This function will do the following:
#Given an input sequence, we will compute the next arrival time
#From this, we will add on the predicted value from the lag_times function output
#Then, we will treat this as our new sequence
#We will continue in this manner until the desired location has been reached


# - Lookup Tables - #
stations <- read.csv("station_lookup_table.csv")

# - User Inputs - #
#start_loc #some string here
#end_loc #some other location here
#desired_depart_time #some string here to coerce to a real number
#desired_arrival_time #some other string here to coerce to a real number
#depart_am #some string here

#example for concreteness
start_loc <- "WOAK"
end_loc <- "EMBR"
desired_depart_time <- "7:15"
depart_am <- "AM"


# - User Input Manipulation - #
start_loc2 <- as.character(stations$r_name[as.character(stations$station_name) == as.character(start_loc)])
end_loc2 <- as.character(stations$r_name[as.character(stations$station_name) == as.character(end_loc)])
desired_depart_time2 <- time_stamp_fixer(desired_depart_time)
#desired_arrival_time <- time_stamp_fixer(desired_arrival_time)
depart_am2 <- as.logical(depart_am=="AM") #turn into a 1 if am, otherwise coerce to 0
locs <- c(start_loc2, end_loc2)

# Grabbing the correct value from the lag_times model

pre_array <- cbind(reg_coef,c(1, rep(0,36)))
names(pre_array)[4] <- "include"
pre_array$include[pre_array$features %in% locs] <- 1
pre_array$include[is.element(as.character(pre_array$features),"am")] <- depart_am2 
pre_array$include[is.element(as.character(pre_array$features),"arrival_time")] <- desired_depart_time2

#View(pre_array)

lag_pred <- sum(pre_array$include*pre_array$coefficients)
#7.25*(-3.24*(10^-4))-(2.59*(10^-3))-(4.5*(10^-4))-(1.17*(10^-3))+(2.417*(10^-2))

#now, get time from rock to mcar
arr_pred <- next_seq_term(c(6.24, 6.34,6.44,7.01), 10, train_set_times, train_set_between)

#add the lag to this 
depart_pred <- arr_pred + lag_pred 

#append this to the old sequence
new_seq <- c(c(6.24, 6.34,6.44,7.01),depart_pred)

#pass this to the next arrival time
arr_pred <- next_seq_term(new_seq, 10, train_set_times, train_set_between)

#compute new lag
#need to take old start location and increment by 1

loc_list <- as.character(stations$station_name)
index_start_loc <- which(loc_list %in% start_loc)
index_start_loc <- index_start_loc + 1
start_loc_new <- loc_list[index_start_loc]
#Ok, I have the idea now. So, I can fully automate this 

# - Automated Process - #
#Copying this again for sanity
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
  #print value for now - will pass to object later
  print(depart_pred)
  #update with new info to continue predictions
  current_seq <- c(current_seq,depart_pred)
  index_start_loc <- index_start_loc + 1 
}