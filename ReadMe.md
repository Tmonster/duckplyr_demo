#

### Getting the data

This will grab about 65 GB of data 
```sql
create table nyc_taxi as 
	select * from read_parquet('s3://voltrondata-labs-datasets/nyc-taxi/*', hive_partitioning=TRUE)
COPY nyc_taxi to 'nyc-taxi' (FORMAT PARQUET, PARTITION_BY (year, month));
-- also need the zone lookup table for some questions
create table zone_lookups as select * from 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv';
```

### Running the scripts

Let us ask the following questions.

1. What is the median tip amount grouped by (day of the week, hour)
2. What is the median tip amount grouped by the number of passenger
3. What is the median tip amount grouped by trip distance (per mile)
4. What pickup neighborhoods tip the most?
5. What percentage of people aren't tipping grouped by (pickup, dropoff) borough
<!-- This one is cool -->
6. What airport dropoff gives you the most tips?
7. -- Tipping in winter months compared to summer months
8. -- What borough to borough trips are the most popular?
9. What are the most popular manhattan to manhattan cab rides? (maybe we want to build a new train line)
10. Does anybody take a taxi to ellis island?


#### Interesting things to note compared to arrow
1. For certain aggregates, you need to call collect(), duckplyr doesn't need this
2. If you are joining two files on a int32 column, and int64 column, you don't need to manually cast the int32 to int64, duckplyr will do this for you.

First show vector exhaustion

In dplyr lets look at rides that cost more than $5 and group them by day of the week then hour. Do people tip more on friday nights? Less on Sunday mornings?

```
Rscript dplyr-vector-exhaustion.R
```
dplyr runs into vector memory exhaustion. The same query with duckplyr does not


```
Rscript duckplyr_can_read.R
```

Let's filter to trips over $50 in the month of december and in november (people are more thankful or giving). Let's do dplyr first.
```
Rscript dplyr-tips.R
```
We do 2 queries so we can show the speed of dplyr once the data has all been loaded into memory.
```
First Query (december)
25.161s
Second Query (november)
6.271s
```


```
Rscript duckplyr_tips.R
First query (december)
3.409s
Second query (november)
3.228
```

Clearly duckplyr is faster. But this is only for fares over $50. Let's see if we can do fares over $2 dollars.
```
Rscript duckplyr_tips_all.R
First query (december)
4.798s
Second query (november)
4.562s
```





Timing results

total amount of trip > 10 and month = 12
tips (by day & hour)

168 groups (7 day per week * 24 hours)

Duckplyr: 4.582 total
dplyr: # (can't even run on my machine)

Duckplyr: 3.237s
dplyr: 4.123s


