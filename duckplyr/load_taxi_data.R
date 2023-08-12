if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckplyr_df_from_file("taxi-data-2019-partitioned/*/*.parquet", "read_parquet", list(hive_partitioning = TRUE)) |>
    filter(month > 9)
  zone_map <- duckplyr_df_from_file("zone_lookups.parquet", "read_parquet")
}
