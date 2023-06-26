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

# vector memory limit exhausted?
# add filter(month==12, total_amount >= 50) |> 
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

print("time to get result")
print(time)