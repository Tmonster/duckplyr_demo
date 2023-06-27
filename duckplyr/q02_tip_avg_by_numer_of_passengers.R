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

tips_by_passenger <- taxi_data_2019 |>
  filter(total_amount > 0) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    n = n(),
    .by = passenger_count
  ) |>
  arrange(desc(passenger_count))

time <- system.time(collect(tips_by_passenger))

# duckdb:::rel_explain(duckdb:::rel_from_altrep_df(tips_by_passenger))