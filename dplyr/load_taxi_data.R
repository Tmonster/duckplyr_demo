if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckdb:::sql("FROM 'taxi-data-2019-partitioned/*/*.parquet' where month == 12")
  zone_map <- duckdb:::sql("FROM 'zone_lookups.parquet'")
}
