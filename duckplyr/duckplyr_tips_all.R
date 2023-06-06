options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

duckplyr_from_parquet <- function(path, options=list()) {
   out <- duckdb:::rel_from_table_function(duckplyr:::get_default_duckdb_connection(), "read_parquet", list(path), options)
   duckplyr:::meta_rel_register_csv(out, path)
   duckplyr:::as_duckplyr_df(duckdb:::rel_to_altrep(out))
}

taxi_data_2019 <- duckplyr_from_parquet('/Users/tomebergen/taxi-data-2019/*/*.parquet', list(hive_partitioning=TRUE))

result_1 <- taxi_data_2019 |> filter(total_amount > 2) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = dayofweek(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==12) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(dn, hr) 

duckdb:::rel_explain(duckdb:::rel_from_altrep_df(result_1))

message("FIRST QUERY month = 12")
system.time(res_materialized <- collect(result_1))


result_2 <- taxi_data_2019 |> filter(total_amount > 2) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = dayofweek(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==11) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  arrange(dn, hr) 

print("SECOND QUERY month = 11")
system.time(res_materialized <- collect(result_2))
