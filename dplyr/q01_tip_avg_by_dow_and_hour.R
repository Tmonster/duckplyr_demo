options(conflicts.policy = list(warn = FALSE))
library(tidyverse)


taxi_data_2019 <- arrow::read_parquet("/Users/tomebergen/duckdb/big-taxis.parquet")
zone_map <- arrow::read_parquet("/Users/tomebergen/duckplyr_demo/zone_lookups.parquet")



# maybe vector memory limit is exhausted depending on your memory?
# there are 168 groups
tips_by_day_hour <- taxi_data_2019 |> 
  filter(total_amount > 0) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr=hour(pickup_datetime)) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |> arrange(desc(avg_tip_pct))

time <- system.time(collect(tips_by_day_hour))