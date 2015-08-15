################################# 
### --- ML - Exploration --- ###
#################################

#===============#
# - Workspace - #
#===============#
path <- "C:/Users/Nitin/Desktop/MIDS/DS210/2.5_months_BART"
setwd(path)

#==============#
# - Packages - #
#==============#
library(forecast) #for auto.arima

#===================#
# - Data Cleaning - #
#===================#
data <- read.csv("clean_train.csv")

#check data types
for (k in 1:ncol(data)) {
  print(class(data[,k]))
}
#all are factors except the first

#need to coerce the 2nd til the end to be characters to pick out appropriate time stamps
for (k in 2:ncol(data)) {
  data[,k] <- as.character(data[,k])
  print(k)
}

#sanity check
for (k in 1:ncol(data)) {
  print(class(data[,k]))
}

#creating column that holds the month, day, and year information
data$month <- 0
data$day <- 0
data$year <- 0

for (k in 1:nrow(data)) {
  data$month[k] <- as.integer(strsplit(data[k,2],"/| ")[[1]][1])
  data$day[k] <- as.integer(strsplit(data[k,2],"/| ")[[1]][2])
  data$year[k] <- as.integer(strsplit(data[k,2],"/| ")[[1]][3])
  print(k)
}

#keeping only the time stamp information in the orginal fields
for (k in 1:nrow(data)) {
  for (j in 2:10) {
    data[k,j] <- strsplit(data[k,j],"/| ")[[1]][4]
  }
  print(c(k,j))
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
  print(c(k,j))
}

#Method 2: We will keep track of the time between stops

data_between <- data #make a copy of the data

for (k in 1:nrow(data_numeric)) {
  for (j in 3:10) {
    data_between[k,j] <- round(60*(as.numeric(data_numeric[k,j])-as.numeric(data_numeric[k,(j-1)])))
  }
  print(c(k,j))
}

#by convention, we will make the first time stamp 0
data_between[,2] <- 0

#=======================================#
### --- Nearest Neighbor Approach --- ###
#=======================================#

#Split into train and test (80-20 split)

## 80% of the sample size
smp_size <- floor(0.80 * nrow(data_numeric))

## set the seed to make your partition reproductible
set.seed(123)
train_ind <- sample(seq_len(nrow(data_numeric)), size = smp_size)

train <- data_numeric[train_ind, ]
test <- data_numeric[-train_ind, ]

#========================#
# - Distance functions - #
#========================#

euclidean_dist <- function(a,b) {
  return(sqrt(sum((a-b)^2)))
}

cosine_sim <- function(a,b) {
  numerator <- sum(a*b)
  denominator <- sqrt(sum(a^2)) * sqrt(sum(b^2))
  return(numerator/denominator)
}

#=========================#
# - Prediction Function - #
#=========================#

knn_reg <- function(input, k, training_set, euc_dist, weight_method) {
  #compute distances
  input_length <- length(input)
  distances <- rep(0,nrow(training_set))
  if (euc_dist == T) {
    for (w in 1:nrow(training_set)) {
      distances[w] <- euclidean_dist(input,as.numeric(training_set[w,1:input_length]))
    }
  } else {
    for (w in 1:nrow(training_set)) {
      distances[w] <- cosine_sim(input,as.numeric(training_set[w,1:input_length]))
    }
  }
  new_train <- cbind(training_set, distances)
  colnames(new_train)[ncol(new_train)] <- "dist"
  
  #keep only the k smallest distances
  keepers <- new_train[order(new_train$dist),][1:k,]
  
  #predict the remainder of the vector by weight_method
  keepers$weight <- rep(1/k, k) #initialize to be uniform
  pred_length <- ncol(training_set) - input_length
  pred_indices <- seq((input_length+1), ncol(training_set))
  pred_vals <- c()
  if (weight_method == "uniform") {
    for (z in pred_indices) {
      pred_vals <- c(pred_vals, sum(keepers$weight*as.numeric(keepers[,z])))
    }
  } else {
    keepers$weight <- 1 - (keepers$dist)/sum(keepers$dist)
    keepers$weight <- keepers$weight / sum(keepers$weight)
    #keepers$weight <- ((keepers$dist)/sum(keepers$dist)^-1) / sum((keepers$dist)/sum(keepers$dist))
    for (z in pred_indices) {
      pred_vals <- c(pred_vals, sum(keepers$weight*as.numeric(keepers[,z])))
    }
  }
  #return(keepers)
  return(pred_vals)
}

#Sanity checking to make sure function works
knn_reg(c(6,6.02), 5, train[,2:10], T, "uniform")
knn_reg(c(6,6.02), 5, train[,2:10], T, "distance")
knn_reg(c(6,6.02), 5, train[,2:10], F, "uniform")
knn_reg(c(6,6.02), 5, train[,2:10], F, "distance")

as.numeric(test[1,2:10])
as.numeric(test[1,6:10])
knn_reg(as.numeric(test[1,2:5]), 10, train[,2:10], T, "uniform")


#===========================#
# - Testing the algorithm - #
#===========================#

#Euclidean distance and uniform weight
s <- rep(0,5)

for (x in 1:nrow(test)) {
  given <- as.numeric(test[x,2:5])
  actual <- as.numeric(test[x,6:10])
  s <- s + (knn_reg(given, 10, train[,2:10], T, "uniform") - actual)^2
  print(x)
}

rmse <- sqrt((1/nrow(test))*s)
print(rmse)

#==================================#
# - Dynamic Probabilistic System - #
#==================================#

dyn_prob_sys <- function(input, k, training_set_times, training_set_between) {
  #first, find the length of the input and of the total path
  input_length <- length(input)
  start_length <- length(input) #trust me, we will need this
  total_length <- ncol(training_set_times)
  #computation
  while (input_length < total_length) {
    #find the k closest sequences
    distances <- rep(0,nrow(training_set_times))
    for (w in 1:nrow(training_set_times)) {
      distances[w] <- euclidean_dist(input,as.numeric(training_set_times[w,1:input_length]))
    }
    new_train_times <- cbind(training_set_times, distances)
    colnames(new_train_times)[ncol(new_train_times)] <- "dist"
    keepers_times <- new_train_times[order(new_train_times$dist),][1:k,]
    keepers_between <- training_set_between[order(new_train_times$dist),][1:k,]
    #compute mean of the next time betweens
    avg_time_between <- mean(as.numeric(as.character(keepers_between[,(input_length+1)])))
    #add this time to the most recent time
    new_time <- as.numeric(input[input_length]) + as.numeric(avg_time_between)/60
    #append output to input, and compute new input length
    input <- c(input, new_time)
    input_length <- length(input)
  }
  return(input[(start_length+1):input_length])
}

train_set_times <- train[,2:10]
train_set_between <- data_between[train_ind,2:10]

knn_reg(as.numeric(test[1,2:5]), 10, train[,2:10], T, "uniform")
dyn_prob_sys(as.numeric(test[1,2:5]), 10, train_set_times, train_set_between)
test[1,6:10]

#===========================#
# - Testing the Algorithm - #
#===========================#
s_2 <- rep(0,5)

for (x in 1:nrow(test)) {
  given <- as.numeric(test[x,2:5])
  actual <- as.numeric(test[x,6:10])
  s_2 <- s_2 + (dyn_prob_sys(given, 10, train_set_times, train_set_between) - actual)^2
  print(x)
}

rmse_2 <- sqrt((1/nrow(test))*s_2)
print(rmse_2)

my_ts <- ts(as.numeric(as.character(test[1,2:5])))
plot(my_ts)
fit <- auto.arima(my_ts)
pred <- forecast(fit)
plot(pred)
