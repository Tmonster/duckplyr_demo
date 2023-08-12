# RUN ALL QUERIES DUCKPLYR
options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckplyr)

source("duckplyr/load_taxi_data.R")

source("duckplyr/q01_tip_avg_by_dow_and_hour.R")
source("duckplyr/q02_tip_avg_by_numer_of_passengers.R")
source("duckplyr/q03_popular_manhattan_cab_rides.R")
source("duckplyr/q04_number_of_no_tip_trips.R")
# source('duckplyr/q05_read_hive_partitioned_files.R')
