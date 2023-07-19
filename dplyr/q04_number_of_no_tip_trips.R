options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckdb)

source('dplyr/load_taxi_data.R')

time1 <- system.time(num_trips_per_borough <- taxi_data_2019 |>
  filter(total_amount > 0) |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  mutate(pickup_borough =Borough.x, dropoff_borough=Borough.y) |>
  select(pickup_borough, dropoff_borough, tip_amount) |>
  summarise(
    num_trips = n(),
    .by = c(pickup_borough, dropoff_borough)
  ))

time2 <- system.time(num_trips_per_borough_no_tip <- taxi_data_2019 |>
  filter(total_amount > 0, tip_amount == 0) |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  mutate(pickup_borough =Borough.x, dropoff_borough=Borough.y, tip_amount) |>
  summarise(
    num_zero_tip_trips = n(),
    .by = c(pickup_borough, dropoff_borough)
  ))

time3 <- system.time(num_zero_percent_trips <- num_trips_per_borough |>
  inner_join(num_trips_per_borough_no_tip) |>
  mutate(num_trips = num_trips, percent_zero_tips_trips = 100*num_zero_tip_trips/num_trips) |> 
  select(pickup_borough, dropoff_borough, num_trips, percent_zero_tips_trips) |>
  arrange(desc(percent_zero_tips_trips)))


q4_dplyr <- time1 + time2 + time3
print("Dplyr Q4 collection time")
print(q4_dplyr)
print("Percentage of trips that report no tip")
num_zero_percent_trips |> head(20) |> print()

rm(num_zero_percent_trips)