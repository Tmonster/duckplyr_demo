# RUN ALL QUERIES DPLYR
options(conflicts.policy = list(warn = FALSE))
library(tidyverse)

source('dplyr/load_taxi_data.R')

source('dplyr/q01_tip_avg_by_dow_and_hour.R')
source('dplyr/q02_tip_avg_by_numer_of_passengers.R')
source('dplyr/q03_popular_manhattan_cab_rides.R')
source('dplyr/q04_number_of_no_tip_trips.R')
