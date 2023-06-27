options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckdb)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckdb:::sql("FROM read_parquet('taxi-data-2019-partitioned/*/*.parquet', hive_partitioning=true)")
  zone_map <- duckdb:::sql("FROM 'zone_lookups.parquet'")
}

tips_by_day_hour <- taxi_data_2019 |> 
  # filter(month==12) |>
  filter(total_amount > 0) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |> arrange(desc(avg_tip_pct))

time <- system.time(collect(tips_by_day_hour))