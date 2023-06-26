options(conflicts.policy = list(warn = FALSE))
library(tidyverse)


taxi_data_2019 <- arrow::read_parquet("/Users/tomebergen/duckdb/big-taxis.parquet")
zone_map <- arrow::read_parquet("/Users/tomebergen/duckplyr_demo/zone_lookups.parquet")

# vector memory limit exhausted?
# add filter(month==12, total_amount >= 50) |> 
tips_by_pickup_neighborhood <- taxi_data_2019 |>
  inner_join(zone_map, by=join_by(pickup_location_id == LocationID)) |>
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  select(Zone, tip_pct) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    .by = Zone
  ) |>
  arrange(desc(avg_tip_pct)) |> head() |>
  print()

print("time to get result")
print(time)