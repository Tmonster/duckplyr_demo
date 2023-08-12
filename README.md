# Organization
1. Getting Started

### Getting Started

First setup and install the libraries and unzip the parquet data
```
### Install package and dependencies
# install.packages("pak", repos = sprintf("https://r-lib.github.io/p/pak/stable/%s/%s/%s", .Platform$pkgType, R.Version()$os, R.Version()$arch))
pak::pak("duckdblabs/duckplyr")

### Download and unzip data (1.7 GB)
curl::curl_download("http://duckplyr-demo-taxi-data.s3-website-eu-west-1.amazonaws.com/taxi-data-2019-partitioned.zip", "taxi-data-2019-partitioned.zip", quiet = FALSE)
unzip("taxi-data-2019-partitioned.zip")
```

### Running the queries/scripts

To run all duckplyr queries at once run 
```
Rscript duckplyr/run_all_queries.R
```
To run all dplyr queries at once run 
```
Rscript dplyr/run_all_queries.R
```

To run just one duckplyr query run
```
Rscript duckplyr/q0*_**.R
```

To run just one dplyr query run
```
Rscript dplyr/q0*_**.R
```

### What do the queries show/highlight?
Q1. Highlights Duckplyr handling of many small groups
    - Get median tips by day & hour. 
    - 168 small groups.
    - Utilizes Perfect hash groups
Q2. Highlights Duckplyr projection pushdown
    - Gets median tip by the number of passengers
    - explain output shows only total_amount, passenger_count, tip_amount, and month are read from the parquet file.
Q3. Highlights Duckplyr filter pushdown. 
    - Gets popular (pickup, drop-off) combinations in Manhattan. 
    - DuckDB can push the filter (Borough = “Manhattan”) all the way into the parquet scan of the dimension table
Q4. Highlights Duckplyr lazy evaluation, 
    - Gets percentage of trips that report no tip. Grouped by (pickup borough, drop-off borogh), ranked by number of trips.
    - Need to join 2 intermediate results,
    - Duckplyr lazily evaluates. 
Q5. Highlights Duckplyr that Duckplyr can read hive partitioned data over the network easy. (Dplyr cannot do this)
    - Hive partition filters
    - Month filter not in explain output (yet)


