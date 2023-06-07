library(dplyr)
library(arrow)

taxi_data_2019 <- read_parquet('/Users/tomebergen/2019-taxi.parquet')


# average tip percentage by number of passengers where amount is over $100
taxi_data_2019 |> filter(total_amount > 100) |>
  select(tip_amount, total_amount, passenger_count) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  group_by(passenger_count) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
  ) |>
  arrange(passenger_count) |>
  collect() |>
  print()


# average trip distance where tip percentage is over 20%
taxi_data_2019 |>
  select(tip_amount, trip_distance, passenger_count, total_amount) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  filter(tip_pct >= 20) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    avg_trip_dist = mean(trip_distance),
  ) |>
  collect() |>
  print()


# average tip amount when trip distance is > 5
taxi_data_2019 |>
  select(tip_amount, trip_distance, passenger_count, total_amount) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  filter(trip_distance > 5) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    avg_trip_dist = mean(trip_distance),
  ) |>
  collect() |>
  print()

# average tip amount when trip distance is > 10
taxi_data_2019 |>
  select(tip_amount, trip_distance, passenger_count, total_amount) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  filter(trip_distance > 10) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    avg_trip_dist = mean(trip_distance),
  ) |>
  collect() |>
  print()

# average tip amount for trips between 0-20km
taxi_data_2019 |>
  select(tip_amount, trip_distance, passenger_count, total_amount) |>
  filter(trip_distance > 0, trip_distance < 20, total_amount > 0) |>
  mutate(tip_pct = 100 * tip_amount / total_amount,
         trip_distance_floor = floor(trip_distance)) |>
  group_by(trip_distance_floor) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n()
  ) |>
  collect() |>
  print()

# how does passenger count relate to pickup drop off time?
# taxi_data_2019 |>
#   select(passenger_count, pickup_datetime) |>
#   mutate(pickup_hour = hour(pickup_datetime)) |>
#   group_by(pickup_hour) |>
#   summarise(
#     num_trips = n()
#   ) |>
#   collect() |>
#   print()

