# RUN ALL QUERIES DPLYR

options(conflicts.policy = list(warn = FALSE))
library(tidyverse)

taxi_data_2019 <- arrow::read_parquet("/Users/tomebergen/duckdb/big-taxis.parquet")
zone_map <- arrow::read_parquet("/Users/tomebergen/duckplyr_demo/zone_lookups.parquet")

source('dplyr/q01_tip_avg_by_dow_and_hour.R')
source('dplyr/q02_tip_avg_by_numer_of_passengers.R')
source('dplyr/q03_popular_manhattan_cab_rides.R')
source('dplyr/q04_number_of_no_tip_trips_by_borough.R')