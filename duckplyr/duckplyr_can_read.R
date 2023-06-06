options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

duckplyr_from_parquet <- function(path, options=list()) {
   out <- duckdb:::rel_from_table_function(duckplyr:::get_default_duckdb_connection(), "read_parquet", list(path), options)
   duckplyr:::meta_rel_register_csv(out, path)
   duckplyr:::as_duckplyr_df(duckdb:::rel_to_altrep(out))
}

taxi_data_2019 <- duckplyr_from_parquet('/Users/tomebergen/2019-taxi.parquet')

result_1 <- taxi_data_2019 |> filter(total_amount > 5, passenger_count > 0) |>
  mutate(tip_pct = 100 * tip_amount / total_amount, dn = dayofweek(pickup_datetime), hr=hour(pickup_datetime)) |>
  filter(month==12) |>
  summarise(
    avg_tip_pct = mean(tip_pct),
    n = n(),
    .by = c(dn, hr)
  ) |>
  filter(n > 10) |>
  arrange(dn, hr)
 
print("print tips by day of the week and hour")
result_1 |> head()
