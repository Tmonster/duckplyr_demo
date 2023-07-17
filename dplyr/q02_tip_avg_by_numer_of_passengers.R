options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckdb)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckdb:::sql("FROM 'taxi-data-2019.parquet'")
  zone_map <- duckdb::sql("FROM 'zone_lookups.parquet'")
}


tips_by_passenger <- taxi_data_2019 |> 
  filter(total_amount > 0) |> 
  # filter(month == 12) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  filter(month==12) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    n = n(),
    .by = passenger_count
  ) |>
  arrange(desc(passenger_count))

time <- system.time(collect(tips_by_passenger))
