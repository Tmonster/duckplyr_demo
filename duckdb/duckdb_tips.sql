create view taxi_data as (select * from  read_parquet('/Users/tomebergen/taxi-data-2019/*/*.parquet', hive_partitioning=TRUE));
create table zone_lookups as select * from 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv';


select 
	extract(dayofweek from pickup_datetime) as day, 
	extract(hour from pickup_datetime) as hour, 
	median(tip_amount*100/total_amount) as median_tip_pct 
from taxi_data 
where month = 12 and total_amount > 5
group by day, hour 
order by avg_tip_pct desc;



-- pickup locations with the highest median tipping percentage.
select 
	zone.Zone pickup_neighborhood,
	median(tip_amount*100/total_amount) as median_tip_pct 
from taxi_data, zone_lookups zone
where zone.LocationID = pickup_location_id and month=12
group by pickup_neighborhood
order by median_tip_pct desc;


-- (pickup,dropoff) locations where no tips are common
select 
	pickup_zone.Borough pickup,
	dropoff_zone.Borough dropoff,
	(select count(*) from taxi_data where tip_amount == 0) as no_tip_trips,
	count(*) as all_trips,
from taxi_data,
	 zone_lookups pickup_zone,
	 zone_lookups dropoff_zone
where pickup_zone.LocationID = pickup_location_id and
      dropoff_zone.LocationID = dropoff_location_id and
      tip_amount >= 0 and tip_amount IS NOT NULL
group by pickup, dropoff
order by all_trips desc;


-- pickup / dropoff borough with the highest median tipping percentage
select 
	pickup_borough.borough pickup,
	dropoff_borough.borough dropoff,
	median(tip_amount*100/total_amount) as median_tip_pct,
from taxi_data,
	 zones_2_borough pickup_borough,
	 zones_2_borough dropoff_borough
where pickup_borough.LocationID = pickup_location_id and
      dropoff_borough.LocationID = dropoff_location_id and
      tip_amount >= 0 and tip_amount IS NOT NULL
group by pickup, dropoff
order by median_tip_pct desc;

-- pickup / dropoff borough with the highest avg tipping percentage
select 
	pickup_borough.borough pickup,
	dropoff_borough.borough dropoff,
	avg(tip_amount*100/total_amount) as median_tip_pct,
from taxi_data,
	 zones_2_borough pickup_borough,
	 zones_2_borough dropoff_borough
where pickup_borough.LocationID = pickup_location_id and
      dropoff_borough.LocationID = dropoff_location_id and
      tip_amount >= 0 and tip_amount IS NOT NULL
group by pickup, dropoff
order by median_tip_pct desc;


-- pickup / dropoff borough with the highest median tipping percentage
select 
    total_amount, tip_amount,
	pickup_borough.borough pickup,
	dropoff_borough.borough dropoff,
from taxi_data,
	 zones_2_borough pickup_borough,
	 zones_2_borough dropoff_borough
where pickup_borough.LocationID = pickup_location_id and
      dropoff_borough.LocationID = dropoff_location_id and 
      pickup like '%Brooklyn%' and dropoff like '%Brooklyn%';


-- what borough to borough trips are the most popular?
select 
	pickup_borough.borough pickup,
	dropoff_borough.borough dropoff,
    count(*) as num_trips,
from taxi_data,
	 zones_2_borough pickup_borough,
	 zones_2_borough dropoff_borough
where pickup_borough.LocationID = pickup_location_id and
      dropoff_borough.LocationID = dropoff_location_id
group by pickup, dropoff
order by num_trips desc;


-- This actually shows some cool tipping patterns
select 
	-- extract(dayofweek from pickup_datetime) as day, 
	extract(hour from pickup_datetime) as hour, 
	avg(tip_amount*100/total_amount) as avg_tip_pct 
from taxi_data 
where month = 12 and total_amount > 5
group by hour 
order by hour;

-- ┌───────┬────────────────────┐
-- │ hour  │    avg_tip_pct     │
-- │ int64 │       double       │
-- ├───────┼────────────────────┤
-- │     0 │  11.13613950799796 │
-- │     1 │ 10.983453363065104 │
-- │     2 │ 10.706341962615287 │
-- │     3 │ 10.019581270161172 │
-- │     4 │  8.892912119971948 │
-- │     5 │   9.09137323729008 │
-- │     6 │ 10.108621618291952 │
-- │     7 │  11.10201222088765 │
-- │     8 │ 11.410076383170727 │
-- │     9 │ 11.011997491782532 │
-- │    10 │ 10.605294578364507 │
-- │    11 │ 10.459614839262485 │
-- │    12 │ 10.426707684526326 │
-- │    13 │ 10.295965454966211 │
-- │    14 │  10.37059843746666 │
-- │    15 │ 10.327746978590106 │
-- │    16 │ 10.415854198754374 │
-- │    17 │ 10.722364312823494 │
-- │    18 │ 10.961138295774559 │
-- │    19 │ 11.081358170160795 │
-- │    20 │ 11.255566746658236 │
-- │    21 │ 11.378057372970646 │
-- │    22 │ 11.414665834833052 │
-- │    23 │ 11.230447652364957 │
-- ├───────┴────────────────────┤
-- │ 24 rows          2 columns │
-- └────────────────────────────┘