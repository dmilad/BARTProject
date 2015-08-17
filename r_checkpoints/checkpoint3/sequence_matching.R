#=======================#
# - Sequence Mathcing - #
#=======================#

# - Utility Functions - #

euclidean_dist <- function(a,b) {
  return(sqrt(sum((a-b)^2)))
}

# - Prediction Function - #

next_seq_term <- function(input, k, training_set_times, training_set_between) {
  #first, find the length of the input and of the total path
  input_length <- length(input)
  start_length <- length(input) #trust me, we will need this
  total_length <- ncol(training_set_times)
  #now, onto the computation 
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
  #output result
  return(input[(start_length+1):input_length])
}
