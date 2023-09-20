library(conflicted)
library(duckplyr)
conflicts_prefer(duckplyr::filter)

taxi_data <- duckplyr_df_from_file(
  "taxi-data-2019-partitioned/*/*.parquet",
  "read_parquet",
  list(hive_partitioning = TRUE)
)

taxi_data_prep <-
  taxi_data |>
  filter(total_amount > 0) |>
  mutate(tip_pct = 100 * tip_amount / total_amount)

tips_by_passenger <-
  taxi_data_prep |>
  summarize(
    .by = passenger_count,
    avg_tip_pct = median(tip_pct),
    n = n(),
  ) |>
  arrange(desc(passenger_count))

class(tips_by_passenger)

explain(tips_by_passenger)

system.time(print(tips_by_passenger$passenger_count))
system.time(print(tips_by_passenger$passenger_count))

tips_by_passenger

system.time(nrow(taxi_data))
