if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- as_duckplyr_df(duckdb:::sql("FROM 'taxi-data-2019-partitioned/*/*.parquet'"))
  zone_map <- as_duckplyr_df(duckdb:::sql("FROM 'zone_lookups.parquet'"))
  taxi_data_2019_lazy <- duckplyr_df_from_file("taxi-data-2019-partitioned/*/*.parquet", "read_parquet", list(hive_partitioning = TRUE))
}
