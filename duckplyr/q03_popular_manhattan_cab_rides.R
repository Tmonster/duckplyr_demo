options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

duckplyr_from_parquet <- function(path, options=list()) {
   out <- duckdb:::rel_from_table_function(duckplyr:::get_default_duckdb_connection(), "read_parquet", list(path), options)
   duckplyr:::meta_rel_register_csv(out, path)
   duckplyr:::as_duckplyr_df(duckdb:::rel_to_altrep(out))
}

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckplyr_from_parquet('taxi-data-2019-partitioned/*/*.parquet', list(hive_partitioning=TRUE))
  zone_map <- duckplyr_from_parquet("../duckplyr_demo/zone_lookups.parquet")
}

popular_manhattan_cab_rides <- taxi_data_2019 |>
  filter(total_amount > 0) |> 
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

# duckdb:::rel_explain(duckdb:::rel_from_altrep_df(popular_manhattan_cab_rides))

print("time to get result")
print(time)