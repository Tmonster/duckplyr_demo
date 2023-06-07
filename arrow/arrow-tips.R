library(tidyverse)
library(arrow)

ds <- open_dataset("/Users/tomebergen/taxi-data-2019/", partitioning = c("month"))
zone_map <- read_parquet("/Users/tomebergen/duckplyr_demo/zone_lookups.parquet")
# zone_map <- read_csv_arrow("/Users/tomebergen/duckplyr_demo/zone_lookups.csv", skip = 1,
#   schema=schema(LocationID=int64(), Borough=string(),Zone=string(),service_zone=string()))
# zone_map[, LocationID] <- lapply(zone_map[, LocationID], as.integer)

# What is the median tip amount grouped by (day of the week, hour)
tips_by_day_hour <- ds |> filter(total_amount > 2) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
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

# What is the median tip amount grouped by the number of passenger
tips_by_passenger <- ds |> filter(total_amount > 2) |> 
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
tips_by_distance <- ds |>
  filter(total_amount > 2, month==12) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, trip_dist_floor = floor(trip_distance)) |>
  # CHECK: this collect makes arrow slow?
  collect() |>
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
tips_by_pickup_neighborhood <- ds |>
  filter(total_amount > 2, month==12) |> 
  full_join(zone_map) |>
  filter(as.integer(pickup_location_id) == as.integer(LocationID)) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  # CHECK: this collect makes arrow slow?
  collect() |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by = pickup_location_id
  ) |>
  select(Zone, avg_tip_pct) |>
  arrange(desc(avg_tip_pct))

print("time to get result")
print(time)


# What percent of taxi rides arent reporting tips / don't tip
# grouped by (pickup, dropoff) Borough
# This requires a window function! Fun
tips_by_pickup_neighborhood <- ds |>
  filter(tip_amount == 0, month==12) |> 
  full_join(zone_map) |>
  filter(as.integer(pickup_location_id) == as.integer(LocationID)) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, trip_dist_floor = floor(trip_distance)) |>
  # CHECK: this collect makes arrow slow?
  collect() |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by = pickup_location_id
  ) |>
  select(Zone, avg_tip_pct) |>
  arrange(desc(avg_tip_pct))

print("time to get result")
print(time)
