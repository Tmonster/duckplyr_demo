options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckdb)

source("dplyr/load_taxi_data.R")

time <- system.time(tips_by_day_hour <- taxi_data_2019 |>
  filter(total_amount > 0) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr = hour(pickup_datetime)) |>
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
tips_by_day_hour |>
  head(5) |>
  print()


# rm(tips_by_day_hour)
