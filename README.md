# duckplyr_demo

## Getting Started

First setup and install the libraries and unzip the parquet data

```r
### Install package and dependencies
# install.packages("pak", repos = sprintf("https://r-lib.github.io/p/pak/stable/%s/%s/%s", .Platform$pkgType, R.Version()$os, R.Version()$arch))
pak::pak(c("duckdblabs/duckplyr", "curl", "zip", "tidyverse"))

### Download and unzip data (1.7 GB)
curl::curl_download("http://duckplyr-demo-taxi-data.s3-website-eu-west-1.amazonaws.com/taxi-data-2019-partitioned.zip", "taxi-data-2019-partitioned.zip", quiet = FALSE)
zip::unzip("taxi-data-2019-partitioned.zip")
```

## Running the queries/scripts

To run all duckplyr queries at once run 

```sh
Rscript duckplyr/run_all_queries.R
```

To run all dplyr queries at once run 

```sh
Rscript dplyr/run_all_queries.R
```

To run just one duckplyr query run

```sh
Rscript duckplyr/q0*_**.R
```

To run just one dplyr query run

```sh
Rscript dplyr/q0*_**.R
```

## What do the queries show/highlight?

1. Highlights duckplyr handling of many small groups

    - Get median tips by day & hour. 
    - 168 small groups.
    - Utilizes Perfect hash groups
    
2. Highlights duckplyr projection pushdown

    - Gets median tip by the number of passengers
    - explain output shows only total_amount, passenger_count, tip_amount, and month are read from the parquet file.

3. Highlights duckplyr filter pushdown. 

    - Gets popular (pickup, drop-off) combinations in Manhattan. 
    - DuckDB can push the filter (Borough = “Manhattan”) all the way into the parquet scan of the dimension table

4. Highlights duckplyr lazy evaluation.

    - Gets percentage of trips that report no tip. Grouped by (pickup borough, drop-off borogh), ranked by number of trips.
    - Need to join 2 intermediate results,
    - duckplyr lazily evaluates. 
    
5. Highlights duckplyr that duckplyr can read hive partitioned data over the network easy. (dplyr cannot do this)

    - Hive partition filters
    - Month filter not in explain output (yet)
