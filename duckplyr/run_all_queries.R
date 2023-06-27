# RUN ALL QUERIES DPLYR
options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckplyr)


duckplyr_from_parquet <- function(path, options=list()) {
   out <- duckdb:::rel_from_table_function(duckplyr:::get_default_duckdb_connection(), "read_parquet", list(path), options)
   duckplyr:::meta_rel_register_csv(out, path)
   duckplyr:::as_duckplyr_df(duckdb:::rel_to_altrep(out))
}

taxi_data_2019 <- duckplyr_from_parquet('/Users/tomebergen/taxi-data-2019/*/*.parquet', list(hive_partitioning=TRUE))
zone_map <- duckplyr_from_parquet("/Users/tomebergen/duckplyr_demo/zone_lookups.parquet")

source('duckplyr/q01_tip_avg_by_dow_and_hour.R')
source('duckplyr/q02_tip_avg_by_numer_of_passengers.R')
source('duckplyr/q03_popular_manhattan_cab_rides.R')
source('duckplyr/q04_number_of_no_tip_trips.R')