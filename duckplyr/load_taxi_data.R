if (!exists("taxi_data_2019")) {
  taxi_data_2019 <- as_duckplyr_df(
    duckdb:::sql("FROM 'taxi-data-2019-partitioned/*/*.parquet' WHERE month IN (10, 11, 12)")
  )
}

if (!exists("zone_map")) {
  zone_map <- as_duckplyr_df(
    duckdb:::sql("FROM 'zone_lookups.parquet'")
  )
}

if (!exists("taxi_data_2019_lazy")) {
  taxi_data_2019_lazy <- duckplyr_df_from_file("taxi-data-2019-partitioned/*/*.parquet", "read_parquet", options = list(hive_partitioning = TRUE))
}

Sys.setenv(DUCKPLYR_FORCE = TRUE)
Sys.setenv(DUCKPLYR_META_SKIP = TRUE)
# Sys.setenv(DUCKPLYR_OUTPUT_ORDER = TRUE) # Too complex, need to move OO preservation to R layer of duckdb
