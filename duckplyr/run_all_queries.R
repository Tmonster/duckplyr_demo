# RUN ALL QUERIES DPLYR
options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

duckplyr_from_parquet <- function(path, options=list()) {
   out <- duckdb:::rel_from_table_function(duckplyr:::get_default_duckdb_connection(), "read_parquet", list(path), options)
   duckplyr:::meta_rel_register_csv(out, path)
   duckplyr:::as_duckplyr_df(duckdb:::rel_to_altrep(out))
}

taxi_data_2019 <- duckplyr_from_parquet('/Users/tomebergen/taxi-data-2019/*/*.parquet', list(hive_partitioning=TRUE))
zone_map <- duckplyr_from_parquet("/Users/tomebergen/duckplyr_demo/zone_lookups.parquet")

taxi_data_2019 <- arrow::read_parquet("/Users/tomebergen/duckdb/big-taxis.parquet")
zone_map <- arrow::read_parquet("/Users/tomebergen/duckplyr_demo/zone_lookups.parquet")

# ---------------  Q1 ---------------
# maybe vector memory limit is exhausted depending on your memory?
# there are 168 groups
tips_by_day_hour <- taxi_data_2019 |> 
  filter(total_amount > 0) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |> arrange(desc(avg_tip_pct))

time <- system.time(collect(tips_by_day_hour))

# ---------------  Q2 ---------------
tips_by_passenger <- taxi_data_2019 |> filter(total_amount > 2) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  filter(month==12) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    n = n(),
    .by = passenger_count
  ) |>
  arrange(desc(passenger_count))

time <- system.time(collect(tips_by_passenger))

# ---------------  Q3 ---------------
tips_by_distance <- taxi_data_2019 |>
  filter(total_amount > 2, month==12) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, trip_dist_floor = floor(trip_distance)) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    n = n(),
    .by = trip_dist_floor
  ) |>
  arrange(desc(avg_tip_pct))

time <- system.time(collect(tips_by_passenger))

# ---------------  Q4 ---------------

tips_by_pickup_neighborhood <- taxi_data_2019 |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  select(Zone, tip_pct) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by = Zone
  ) |>
  arrange(desc(avg_tip_pct)) |> head() |>
  print()

time <- system.time(collect(tips_by_pickup_neighborhood))

# ---------------  Q5 ---------------

num_trips_per_borough <- taxi_data_2019 |>
  filter(total_amount > 40) |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  mutate(pickup_borough =Borough.x, dropoff_borough=Borough.y) |>
  select(pickup_borough, dropoff_borough, tip_amount) |>
  summarise(
    num_trips_with_0_tip = n(),
    num_trips = n(),
    .by = c(pickup_borough, dropoff_borough)
  )

num_trips_per_borough_no_tip <- taxi_data_2019 |>
  filter(total_amount > 40) |>
  filter(tip_amount == 0) |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  mutate(pickup_borough =Borough.x, dropoff_borough=Borough.y, tip_amount) |>
  summarise(
    num_zero_tip_trips = n(),
    .by = c(pickup_borough, dropoff_borough)
  )

num_zero_percent_trips <- num_trips_per_borough |>
  inner_join(num_trips_per_borough_no_tip) |>
  mutate(num_trips = num_trips, percent_zero_tips_trips = 100*num_zero_tip_trips/num_trips) |> 
  select(pickup_borough, dropoff_borough, num_trips, percent_zero_tips_trips) |>
  arrange(desc(percent_zero_tips_trips)) |> print()

time <- system.time(collect(num_zero_percent_trips))