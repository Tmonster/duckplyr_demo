options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckplyr_df_from_file('taxi-data-2019-partitioned/*/*.parquet', 'read_parquet', list(hive_partitioning=TRUE))
  zone_map <- duckplyr_df_from_file("zone_lookups.parquet", 'read_parquet')
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

# duckplyr::rel_explain(duckplyr::duckdb_rel_from_df(tips_by_passenger))