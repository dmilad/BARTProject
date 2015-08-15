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
start_loc <- "ROCK"
end_loc <- "MCAR"
desired_depart_time <- "7:15"
depart_am <- "AM"


# - User Input Manipulation - #
start_loc <- as.character(stations$r_name[as.character(stations$station_name) == as.character(start_loc)])
end_loc <- as.character(stations$r_name[as.character(stations$station_name) == as.character(end_loc)])
desired_depart_time <- time_stamp_fixer(desired_depart_time)
#desired_arrival_time <- time_stamp_fixer(desired_arrival_time)
depart_am <- as.logical(depart_am=="AM") #turn into a 1 if am, otherwise coerce to 0
locs <- c(start_loc, end_loc)

# Grabbing the correct value from the lag_times model

pre_array <- cbind(reg_coef,c(1, rep(0,36)))
names(pre_array)[4] <- "include"
pre_array$include[pre_array$features %in% locs] <- 1
pre_array$include[is.element(as.character(pre_array$features),"am")] <- depart_am 
pre_array$include[is.element(as.character(pre_array$features),"arrival_time")] <- desired_depart_time

#View(pre_array)

lag_pred <- sum(pre_array$include*pre_array$coefficients)
7.25*(-3.24*(10^-4))-(2.59*(10^-3))-(4.5*(10^-4))-(1.17*(10^-3))+(2.417*(10^-2))

#now, get time from rock to mcar