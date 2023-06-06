options(conflicts.policy = list(warn = FALSE))
library(arrow)
library(tidyverse)

taxi_data_2019 <- read_parquet('/Users/tomebergen/2019-taxi.parquet')

# get average tip amount in december group by passenger count.
q1 <- system.time(taxi_data_2019  |> filter(total_amount > 5, passenger_count > 0) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==12) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(dn, hr) |> print())

print("print tips by day of the week and hour")
q1
