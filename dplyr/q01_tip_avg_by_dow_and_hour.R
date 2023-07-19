options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckdb)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckdb:::sql("FROM 'taxi-data-2019.parquet' where month > 9")
  zone_map <- duckdb:::sql("FROM 'zone_lookups.parquet'")
}

time <- system.time(tips_by_day_hour <- taxi_data_2019 |>
  filter(total_amount > 0) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  mutate(dn = dn - 1) |> 
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |> arrange(desc(avg_tip_pct)))

q1_dplyr <- time
print("Dplyr Q1 collection time")
print(q1_dplyr)
print("Tip Average by day of week and hour")
tips_by_day_hour |> head(5) |> print()


# rm(tips_by_day_hour)