options(conflicts.policy = list(warn = FALSE))
library(tidyverse)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- arrow::read_parquet("taxi-data-2019.parquet")
  zone_map <- arrow::read_parquet("zone_lookups.parquet")
}

popular_manhattan_cab_rides <- taxi_data_2019 |>
  filter(total_amount > 0) |> 
  # filter(month == 12) |>
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

time <- system.time(collect(popular_manhattan_cab_rides))

print("time to get result")
print(time)