options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

options(duckdb.materialize_message = FALSE)

source('duckplyr/load_taxi_data.R')

tips_by_passenger <- taxi_data_2019 |>
  filter(total_amount > 0) |> 
  mutate(tip_pct = 100 * tip_amount / total_amount) |>
  summarise(
    avg_tip_pct = median(tip_pct),
    n = n(),
    .by = passenger_count
  ) |>
  arrange(desc(passenger_count))

time <- system.time(collect(tips_by_passenger))

q2_duckplyr <- time
print("Q2 Duckplyr collection time")
print(q2_duckplyr)
print("tips by passenger count")
tips_by_passenger |> head(5) |> print()

# duckplyr::rel_explain(duckplyr::duckdb_rel_from_df(tips_by_passenger))