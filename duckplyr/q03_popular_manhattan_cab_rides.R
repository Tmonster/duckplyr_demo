options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

options(duckdb.materialize_message = FALSE)

source("duckplyr/load_taxi_data.R")

popular_manhattan_cab_rides <- taxi_data_2019 |>
  filter(total_amount > 0) |>
  inner_join(zone_map, by = join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by = join_by(dropoff_location_id == LocationID)) |>
  filter(Borough.x == "Manhattan", Borough.y == "Manhattan") |>
  select(start_neighborhood = Zone.x, end_neighborhood = Zone.y) |>
  summarise(
    num_trips = n(),
    .by = c(start_neighborhood, end_neighborhood),
  ) |>
  arrange(desc(num_trips))

time <- system.time(collect(popular_manhattan_cab_rides))

# duckplyr::rel_explain(duckplyr::duckdb_rel_from_df(popular_manhattan_cab_rides))
q3_duckplyr <- time
print("Q3 Duckplyr collection time")
print(q3_duckplyr)
print("Most popular cab rides within manhattan")
popular_manhattan_cab_rides |>
  head(5) |>
  print()
