options(conflicts.policy = list(warn = FALSE))
library(tidyverse)
library(duckplyr)

options(duckdb.materialize_message = FALSE)

source("duckplyr/load_taxi_data.R")

# maybe vector memory limit is exhausted depending on your memory?
# there are 168 groups
tips_by_day_hour <- taxi_data_2019 |>
  filter(total_amount > 0) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = wday(pickup_datetime), hr = hour(pickup_datetime)) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(desc(avg_tip_pct))

time <- system.time(collect(tips_by_day_hour))

q1_duckplyr <- time
print("Q1 Duckplyr collection time")
print(q1_duckplyr)
print("Tip Average by day of week and hour")
tips_by_day_hour |>
  head(5) |>
  print()

# duckplyr::rel_explain(duckplyr::duckdb_rel_from_df(tips_by_day_hour))
