if (!exists("taxi_data_2019") && !exists("zone_map")) {
  taxi_data_2019 <- duckdb:::sql("FROM 'taxi-data-2019.parquet' where month > 9")
  zone_map <- duckdb:::sql("FROM 'zone_lookups.parquet'")
}
