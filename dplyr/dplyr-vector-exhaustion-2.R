library(dplyr)
library(arrow)

taxi_data_2019 <- read_parquet('/Users/tomebergen/2019-taxi.parquet')

taxi_data_2019 |> filter(total_amount > 10) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  filter(month==12) |>
  group_by(passenger_count) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
  ) |>
  arrange(passenger_count) |>
  collect() |>
  print()