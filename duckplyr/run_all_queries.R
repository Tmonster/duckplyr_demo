# RUN ALL QUERIES DUCKPLYR
options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckplyr)

source("duckplyr/load_taxi_data.R", echo = TRUE)

source("duckplyr/q01_tip_avg_by_dow_and_hour.R", echo = TRUE)
source("duckplyr/q02_tip_avg_by_numer_of_passengers.R", echo = TRUE)
source("duckplyr/q03_popular_manhattan_cab_rides.R", echo = TRUE)
source("duckplyr/q04_number_of_no_tip_trips.R", echo = TRUE)
# source("duckplyr/q05_read_hive_partitioned_files.R", echo = TRUE)
