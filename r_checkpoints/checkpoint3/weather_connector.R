#==============================#
# - Weather Connector Script - #
#==============================#

# - Setup - #
path <- "C:/Users/Nitin/Desktop/MIDS/DS210/2.5_months_BART"
setwd(path)


library(RMySQL)

mydb = dbConnect(MySQL(), user='sl_user', password='sl_password', dbname='bart_project', host='158.85.198.80')

# - Snooping Around - #

weather_current <- dbGetQuery(mydb, "SELECT * FROM weather_current limit 10")
weather_forecast <- dbGetQuery(mydb, "SELECT * FROM weather_forecast limit 10")

View(weather_current)
