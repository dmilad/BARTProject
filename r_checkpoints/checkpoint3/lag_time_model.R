#====================#
# - Lag Time Model - #
#====================#

#This model outputs the given lag for a particular station

#To construct the model, we use the coefficients from a Ridge Regression

reg_coef <- read.csv("lag_time_coeffs2_L2.csv")
colnames(reg_coef) <- c("X","features", "coefficients")
length(reg_coef$features) #input array needs to be length 37

#Model
#will take an input array of 0-1's for each feature and output lag time

lag_times <- function(array) {
  return(sum(as.numeric(as.character(reg_coef$coefficients[as.logical(array)]))))
}
