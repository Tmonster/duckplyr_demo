options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

options(duckdb.materialize_message = FALSE)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckplyr_df_from_file('taxi-data-2019-partitioned/*/*.parquet', 'read_parquet', list(hive_partitioning=TRUE))
  zone_map <- duckplyr_df_from_file("zone_lookups.parquet", 'read_parquet')
}

# -------- Q4 ---------
# What percent of taxi rides per borough arent reporting tips / don't tip
# grouped by (pickup, dropoff) Borough

# What pickup neighborhoods tip the most?
num_trips_per_borough <- taxi_data_2019 |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  mutate(pickup_borough =Borough.x, dropoff_borough=Borough.y) |>
  select(pickup_borough, dropoff_borough, tip_amount) |>
  summarise(
    num_trips = n(),
    .by = c(pickup_borough, dropoff_borough)
  )

num_trips_per_borough_no_tip <- taxi_data_2019 |>
  filter(tip_amount == 0) |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  inner_join(zone_map, by=join_by(dropoff_location_id == LocationID)) |>
  mutate(pickup_borough =Borough.x, dropoff_borough=Borough.y, tip_amount) |>
  summarise(
    num_zero_tip_trips = n(),
    .by = c(pickup_borough, dropoff_borough)
  )

num_zero_percent_trips <- num_trips_per_borough |>
  inner_join(num_trips_per_borough_no_tip, by=join_by(pickup_borough, dropoff_borough)) |>
  mutate(num_trips = num_trips, percent_zero_tips_trips = 100*num_zero_tip_trips/num_trips) |> 
  select(pickup_borough, dropoff_borough, num_trips, percent_zero_tips_trips) |>
  arrange(desc(num_trips))


time <- system.time(collect(num_zero_percent_trips))

print("Q4 collection time")
print(time)
num_zero_percent_trips |> head(5) |> print()



# duckplyr::rel_explain(duckplyr::duckdb_rel_from_df(num_zero_percent_trips))