#=========================#
# - Lag Time Prediciton - #
#=========================#

#===============#
# - Workspace - #
#===============#

path <- "C:/Users/Nitin/Desktop/MIDS/DS210/2.5_months_BART"
setwd(path)

#data <- read.csv("lag_times.csv")
#data <- read.csv("lag_times_ROCK.csv")
data <- read.csv("lag_times_ROCK2.csv")
#=========================#
# - Feature Engineering - #
#=========================#
View(head(data))

# - FEATURE 1 - #
#binarize train length, since these are essentially catagorical
table(data$train_length)

#Note, there are some entries that have a train length of 0 - remove these fromt the analysis
data <- data[data$train_length!=0,]

#Binarizing
#data$l_4 <- as.numeric(data$train_length==4)
#data$l_5 <- as.numeric(data$train_length==5)
#data$l_6 <- as.numeric(data$train_length==6)
#data$l_7 <- as.numeric(data$train_length==7)
#data$l_8 <- as.numeric(data$train_length==8)
#data$l_9 <- as.numeric(data$train_length==9)
#data$l_10 <- as.numeric(data$train_length==10)
data$l_1 <- as.numeric(data$train_length==1)
data$l_2 <- as.numeric(data$train_length==2)
data$l_4 <- as.numeric(data$train_length==4)
data$l_5 <- as.numeric(data$train_length==5)
data$l_6 <- as.numeric(data$train_length==6)
data$l_7 <- as.numeric(data$train_length==7)
data$l_8 <- as.numeric(data$train_length==8)
data$l_9 <- as.numeric(data$train_length==9)
data$l_10 <- as.numeric(data$train_length==10)


# - FEATURE 2 - #
#binarize stations
table(data$station)

data$s_12 <- as.numeric(data$station=="12TH")
data$s_16 <- as.numeric(data$station=="16TH")
data$s_19 <- as.numeric(data$station=="19TH")
data$s_24 <- as.numeric(data$station=="24TH")
data$s_balb <- as.numeric(data$station=="BALB")
data$s_civc <- as.numeric(data$station=="CIVC")
data$s_colm <- as.numeric(data$station=="COLM")
data$s_conc <- as.numeric(data$station=="CONC")
data$s_daly <- as.numeric(data$station=="DALY")
data$s_embr <- as.numeric(data$station=="EMBR")
data$s_glen <- as.numeric(data$station=="GLEN")
data$s_lafy <- as.numeric(data$station=="LAFY")
data$s_mcar <- as.numeric(data$station=="MCAR")
data$s_mlbr <- as.numeric(data$station=="MLBR")
data$s_mont <- as.numeric(data$station=="MONT")
data$s_ncon <- as.numeric(data$station=="NCON")
data$s_orin <- as.numeric(data$station=="ORIN")
data$s_phil <- as.numeric(data$station=="PHIL")
data$s_powl <- as.numeric(data$station=="POWL")
data$s_rock <- as.numeric(data$station=="ROCK")
data$s_sbrn <- as.numeric(data$station=="SBRN")
data$s_sfia <- as.numeric(data$station=="SFIA")
data$s_ssan <- as.numeric(data$station=="SSAN")
data$s_wcrk <- as.numeric(data$station=="WCRK")
data$s_woak <- as.numeric(data$station=="WOAK")

# - FEATURE 3 - #
#am or pm, along with arrival time
data$am <- as.numeric(as.character(data$am_pm)=="AM")
data$arrival_time <- data$time

#========================#
# - Practical Concerns - #
#========================#

x <- as.matrix(data[,7:ncol(data)], sparse = T)
y <- data$lag

corr_matrix <- cor(x)
library('corrplot') 
corrplot(corr_matrix, method = "circle") #some presense of multicollinearity, so regularize


#========#
# - ML - #
#========#

# - Method 1: Regression - # OLD MODEL - REFINED BELOW
#reg_model <- lm(lag~l_1+l_2+l_4+l_8+l_9+l_10+s_12+s_16+s_19+s_24+s_balb+s_civc+
#                s_conc+s_daly+s_embr+s_glen+s_lafy+s_mcar+s_mont+s_phil+s_powl+s_rock+s_wcrk+s_woak, data=data)

#summary(reg_model)

#We know from our correlation plot that there is some multicollinearity

# - Method 2: Ridge Regression - #

library(glmnet)

elastic_reg_model <- glmnet(x,y, alpha = 0)
summary(elastic_reg_model)
plot(elastic_reg_model, xlab = "L2 Norm")

#Perform CV to find optimal parameter for regularization
cv_fit <- cv.glmnet(x,y, alpha = 0)
plot(cv_fit, main = "L2 Regularization")
minimum_lambda <- cv_fit$lambda.min
print(minimum_lambda) #0.04782516

#Print out the coefficients
coefficients <- coef(cv_fit, s = "lambda.min")
coefficients <- as.numeric(coefficients)
coefficients <- data.frame(c("intercept",colnames(data[,7:ncol(data)])),coefficients)
View(coefficients)

write.csv(coefficients, "lag_time_coeffs_L2.csv")

# - Method 3: Lasso Regression - #

elastic_reg_model <- glmnet(x,y, alpha = 1)
summary(elastic_reg_model)
plot(elastic_reg_model, xlab = "L1 Norm")

#Perform CV to find optimal parameter for regularization
cv_fit <- cv.glmnet(x,y, alpha = 1)
plot(cv_fit, main = "L1 Regularization")
minimum_lambda <- cv_fit$lambda.min
print(minimum_lambda) #0.000193071

#Print out the coefficients
coefficients <- coef(cv_fit, s = "lambda.min")
coefficients <- as.numeric(coefficients)
coefficients <- data.frame(c("intercept",colnames(data[,7:ncol(data)])),coefficients)
View(coefficients)

write.csv(coefficients, "lag_time_coeffs_L1.csv")

