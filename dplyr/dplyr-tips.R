options(conflicts.policy = list(warn = FALSE))
library(arrow)
library(tidyverse)
library(duckdb)

taxi_data_2019 <- duckdb:::sql("FROM '/Users/tomebergen/taxi-data-2019/*/*.parquet'")

q1 <- system.time(taxi_data_2019  |> filter(total_amount > 50) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==12) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(dn, hr) |> print())

print("FIRST QUERY month = 12")
q1

q2 <- system.time(taxi_data_2019 |> filter(total_amount > 50) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==11) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(dn, hr) |> print())

print("SECOND QUERY month = 11")
q2
