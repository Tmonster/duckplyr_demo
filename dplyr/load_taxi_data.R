if (!exists("taxi_data_2019")) {
  taxi_data_2019 <- duckdb:::sql("FROM 'taxi-data-2019-partitioned/*/*.parquet' WHERE month IN (10, 11, 12)")
}

if (!exists("zone_map")) {
  zone_map <- duckdb:::sql("FROM 'zone_lookups.parquet'")
}
