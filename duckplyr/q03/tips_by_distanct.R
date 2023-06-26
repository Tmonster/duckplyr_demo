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