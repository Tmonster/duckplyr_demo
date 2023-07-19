options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

options(duckdb.materialize_message = FALSE)

source('duckplyr/load_taxi_data.R')

tips_by_day_hour <- taxi_data_2019 |> 
  filter(total_amount > 0) |> 
  filter(month==12) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = dayofweek(pickup_datetime), hr=hour(pickup_datetime)) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(desc(avg_tip_pct))
  

time <- system.time(collect(tips_by_day_hour))

# duckplyr::rel_explain(duckplyr::duckdb_rel_from_df(tips_by_day_hour))

q5_duckplyr <- time
print("Q5 Duckplyr collection time")
print(q5_duckplyr)
