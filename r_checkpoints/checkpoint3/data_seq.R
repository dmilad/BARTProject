#=========================#
# - Data For Sequencing - #
#=========================#

#This script produces all the necesary data for the sequence mathching algorithm

data <- read.csv("clean_train.csv")

#Coerce Data Types
for (k in 2:ncol(data)) {
  data[,k] <- as.character(data[,k])
}

#creating column that holds the month, day, and year information
data$month <- 0
data$day <- 0
data$year <- 0

for (k in 1:nrow(data)) {
  data$month[k] <- as.integer(strsplit(data[k,2],"/| ")[[1]][1])
  data$day[k] <- as.integer(strsplit(data[k,2],"/| ")[[1]][2])
  data$year[k] <- as.integer(strsplit(data[k,2],"/| ")[[1]][3])
}

#keeping only the time stamp information in the orginal fields
for (k in 1:nrow(data)) {
  for (j in 2:10) {
    data[k,j] <- strsplit(data[k,j],"/| ")[[1]][4]
  }
}

#To use many machine learning models, we need the time stamps to be real numbers
#There are 2 ways to remedy this

#Method 1: We will impose the following transforation -> hour:minutes will become hour.(minutes/60)

data_numeric <- data #make a copy of the data

time_stamp_fixer <- function(string) {
  hour <- as.numeric(strsplit(string,":")[[1]][1])
  minutes <- as.numeric(strsplit(string,":")[[1]][2])
  new_time <- paste0(as.character(hour),substr(as.character(minutes/60),2,nchar(as.character(minutes/60))))
  return(as.numeric(new_time))
}

for (k in 1:nrow(data_numeric)) {
  for (j in 2:10) {
    data_numeric[k,j] <- as.numeric(time_stamp_fixer(data_numeric[k,j]))
  }
}

#Method 2: We will keep track of the time between stops

data_between <- data #make a copy of the data

for (k in 1:nrow(data_numeric)) {
  for (j in 3:10) {
    data_between[k,j] <- round(60*(as.numeric(data_numeric[k,j])-as.numeric(data_numeric[k,(j-1)])))
  }
}

#by convention, we will make the first time stamp 0
data_between[,2] <- 0

#Final Transforms 
train_set_times <- data_numeric[,2:10]
train_set_between <- data_between[,2:10]

# - Reverse Time Stamp Function - #
#This will be used to coerce predictions back to time format to display in the UI
reverse_time_stamp <- function(value){
  #takes a real number of the form a.bcdef... and coerces to a:zy
  value <- as.character(value)
  hour <- strsplit(value, "\\.")[[1]][1]
  minutes <- round(as.numeric(substr(strsplit(value, "\\.")[[1]][2],1,2))*60/100)
  if (minutes < 10) {
    minutes <- paste0("0",minutes)
  }
  return(paste0(hour,":",as.character(minutes)))
}

#reverse_time_stamp(6.25) outputs 6:15, which is correct

