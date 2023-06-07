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

print_result <- function(res) {
  print(invisible(res))
}

tips_by_day_hour <- taxi_data_2019 |> filter(total_amount > 2) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = dayofweek(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==12) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(desc(avg_tip_pct))

time <- system.time(collect(tips_by_day_hour))

print("time to get result")
print(time)

# duckdb:::rel_explain(duckdb:::rel_from_altrep_df(result_1))

# What is the median tip amount grouped by the number of passenger
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

print("time to get result")
print(time)

# What is the median tip amount grouped by trip distance (per mile)
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

print("time to get result")
print(time)

# What pickup neighborhoods tip the most?
tips_by_pickup_neighborhood <- taxi_data_2019 |>
  filter(total_amount > 2, month==12) |> 
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  select(Zone, tip_pct) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by = Zone
  ) |>
  arrange(desc(avg_tip_pct)) |> head() |>
  print()

print("time to get result")
print(time)


# What percent of taxi rides arent reporting tips / don't tip
# grouped by (pickup, dropoff) Borough
# This requires a window function! Fun
# What pickup neighborhoods tip the most?
# tips_by_pickup_neighborhood <- taxi_data_2019 |>
#   filter(month==12) |> 
#   inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
#   inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
#   mutate(pickup_borough =Borough.x, dropoff_borough=Borough.y, tip_amount) |>
#   summarise(
#     num_trips = n(),
#     num_trips_no_tip = 
#     .by = c(pickup_borough, dropoff_borough)
#   ) |>
#   arrange(desc(avg_tip_pct)) |> head() |>
#   print()

# print("time to get result")
# print(time)

# What airport dropoff gives you the most tips
tips_by_airport <- taxi_data_2019 |>
  filter(total_amount > 2, month==12) |> 
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  filter(pickup_location_id == 1 | pickup_location_id == 132 | pickup_location_id == 138) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  select(Zone, tip_pct) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by=Zone
  ) |>
  arrange(desc(avg_tip_pct)) |>
  print()

print("time to get result")
print(time)

