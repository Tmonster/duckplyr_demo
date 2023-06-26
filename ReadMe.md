# Organization
1. Getting nyc-taxi data
2. Installation of libraries
3. What solutions can run the queries?
4. Running the queries/scripts
5. What has improved 

### Getting nyc-taxi  data

This will grab about 65 GB of data 
```sql
$ duckdb
create table nyc_taxi as select * from read_parquet('s3://voltrondata-labs-datasets/nyc-taxi/*', hive_partitioning=TRUE)
COPY nyc_taxi to 'nyc-taxi' (FORMAT PARQUET, PARTITION_BY (year, month));
create table zone_map as (SELECT * FROM 'zone_lookups.csv');
```

### Installation of libraries
-- TODO.

### Running the queries/scripts

With all this data, we can ask a lot of questions about how people have been tipping new york taxis. We can ask the following questions.

1. What is the median tip amount grouped by (day of the week, hour)
	- Lots of group bys
	- 7 * 24 = 168 groups
2. What is the median tip amount grouped by number of passengers
	- No everything can be done directly on the parquet files
	- no need to call any intermediate collect().

3. What is the median tip amount grouped by trip distance (per mile)
4. What pickup neighborhoods tip the most?
	- again, lots of grouping and averaging

5. What percentage of people aren't tipping grouped by (pickup, dropoff) borough
	- shows that duckplyr does not eagerly evaluate. 
	- first get the number of trips per boroughs
	- then get number of trips with no tip per borough
	- join them and calculate the result. 
	- (In pure duckdb this is easier with a case statement in the count() function)

6. What airport dropoff gives you the most tips?
7. How does tipping in winter months compare to tipping in summer months?

8. What borough to borough trips are the most popular?
9. What are the most popular manhattan to manhattan cab rides?


10. Does anybody take a taxi to one of the manhattan islands that technically cannot be reached via private vehicles?

### What has improved 
1. For certain aggregates, you need to call `collect()` to bring data into memory, duckplyr doesn't need this
2. When joining two tables, if the columns types are `int32` and `int64`, duckplyr automatically handles upcasting (apparently dplyr as well, but arrow doesn't).
3. Filter pushdown for hive partitioned files (going to land soon).
<!-- 4. Windowing (Q5) -->
4. Projection push down (still needs to be fixed)


