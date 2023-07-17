options(conflicts.policy = list(warn = FALSE))
library(duckplyr)
library(tidyverse)

options(duckdb.materialize_message = FALSE)

if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckplyr_df_from_file('taxi-data-2019-partitioned/*/*.parquet', 'read_parquet', list(hive_partitioning=TRUE))
  zone_map <- duckplyr_df_from_file("zone_lookups.parquet", 'read_parquet')
}

# if (!exists("taxi_data_2019") && !exists("zone_map")) {
#   taxi_data_2019 <- duckplyr_df_from_file('s3://voltrondata-labs-datasets/nyc-taxi-tiny', 'read_parquet', list(hive_partitioning=TRUE))
#   zone_map <- duckplyr_df_from_file("zone_lookups.parquet", 'read_parquet')
# }


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

print("Q5 collection time")
print(time)

tips_by_day_hour |> head(5) |> print()

