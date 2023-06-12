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

# -------- Q1 ---------
# Get median tip percentage, grouped by day by hour. 

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

# -------- Q2 ---------
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


# -------- Q3 ---------
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

# -------- Q4 ---------
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


# -------- Q5 ---------
# What percent of taxi rides per borough arent reporting tips / don't tip
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

# -------- Q6 ---------
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

# -------- Q7 ---------
# How does tipping in winter months (Jan, Feb, March) compare to summer? (June, July, August)
winter_months <- taxi_data_2019 |>
  filter(total_amount > 2, month==1 | month == 2 | month == 3) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by = month
  )
summer_months <- taxi_data_2019 |>
  filter(total_amount > 2, month==6 | month == 7 | month == 8) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by = month
  )

# -------- Q8 ---------
# What borough to borough trips are the most popular?
tips_by_pickup_neighborhood <- taxi_data_2019 |>
  filter(total_amount > 2) |> 
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  select(start_borough = Borough.x, end_borough=Borough.y, tip_pct) |>
  summarise(
    num_trips = n(),
    .by = c(start_borough, end_borough)
  ) |>
  arrange(desc(num_trips)) |> head() |>
  print()

print("time to get result")
print(time)

# -------- Q9 ---------
# What are the most popular manhattan to manhattan cab rides?
manhattan_popular_rides <- taxi_data_2019 |>
  filter(total_amount > 2) |> 
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  filter(Borough.x == "Manhattan", Borough.y=="Manhattan") |>
  select(start_neighborhood = Zone.x, end_neighborhood = Zone.y) |>
  summarise(
    num_trips = n(),
    .by = c(start_neighborhood, end_neighborhood),
  ) |>
  arrange(desc(num_trips)) |> head(20) |>
  print()


# -------- Q10 ---------
# Does anybody go to Ellis island? Not possible technically
num_trips_ending_at_an_island <- taxi_data_2019 |>
  filter(total_amount > 2) |> 
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  # filter pushdown,
  filter(dropoff_location_id == 103 | dropoff_location_id == 104 | dropoff_location_id == 105) |> 
  select(end_neighborhood = Zone.y) |>
  summarise(
    num_trips = n(),
    .by = end_neighborhood,
  ) |> head(20) |>
  print()

num_trips_starting_at_an_island <- taxi_data_2019 |>
  filter(total_amount > 2) |> 
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  # filter pushdown,
  filter(pickup_location_id == 103 | pickup_location_id == 104 | pickup_location_id == 105) |> 
  select(pickup_neighborhood = Zone.x) |>
  summarise(
    num_trips = n(),
    .by = pickup_neighborhood,
  ) |> head(20) |>
  print()

# Interesting 1 pickup at Ellis island. 117 at Governor's island.

