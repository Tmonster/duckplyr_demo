options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckdb)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckdb:::sql("FROM 'taxi-data-2019.parquet' where month > 9")
  zone_map <- duckdb:::sql("FROM 'zone_lookups.parquet'")
}


time <- system.time(tips_by_passenger <- taxi_data_2019 |> 
  filter(total_amount > 0) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    n = n(),
    .by = passenger_count
  ) |>
  arrange(desc(passenger_count)))

q2_dplyr <- time
print("Dplyr Q2 collection time")
print(q2_dplyr)
print("tips by passenger count")
tips_by_passenger |> head(5) |> print()

rm(tips_by_passenger)