## DATA CLEANING IN BIGQUERY

## Data source: 2013 Taxi data from the TLC @ https://bigquery.cloud.google.com/table/imjasonh-storage:nyctaxi.trip_data

# creating one unified flatfile that unifies trip data with fare data and changes schema to proper data types
SELECT medallion, hack_license, vendor_id, rate_code, pickup_datetime, dropoff_datetime, passenger_count,
 trip_time_in_secs, trip_distance, pickup_long, pickup_lat, dropoff_long, dropoff_lat, payment_type, fare_amt,
  surcharge, mta_tax, tip_amt, tolls_amt, total_amt FROM
(SELECT medallion, hack_license, vendor_id, CAST (rate_code AS INT64) AS rate_code, CAST (pickup_datetime AS DATETIME) AS pickup_datetime, CAST (dropoff_datetime AS DATETIME) AS dropoff_datetime,
 CAST (passenger_count AS INT64) AS passenger_count, CAST (trip_time_in_secs AS INT64) AS trip_time_in_secs, CAST (trip_distance AS FLOAT64) AS trip_distance, CAST (pickup_latitude AS FLOAT64) AS pickup_lat,
  CAST (pickup_longitude AS FLOAT64) AS pickup_long, CAST (dropoff_longitude AS FLOAT64) AS dropoff_long, 
  CAST (dropoff_latitude AS FLOAT64) AS dropoff_lat FROM tlc.trip_data_copy) x
INNER JOIN
(SELECT medallion AS medallion2, hack_license AS hack_license2, CAST (pickup_datetime AS DATETIME) AS pickup_datetime2, payment_type, CAST (fare_amount AS FLOAT64) AS
 fare_amt, CAST (surcharge AS FLOAT64) AS surcharge, CAST (mta_tax AS FLOAT64) as mta_tax, CAST (tip_amount AS FLOAT64) AS tip_amt, 
 CAST (tolls_amount AS FLOAT64) AS tolls_amt, CAST (total_amount AS FLOAT64) AS total_amt FROM tlc.trip_fare_copy) y
ON x.hack_license = y.hack_license2
AND x.pickup_datetime = y.pickup_datetime2
AND x.medallion = y.medallion2

# concatenating hack license and pickup datetime to create some unique field, laying groundwork for deduplicaton
SELECT *, CONCAT (CAST (hack_license AS STRING), CAST (pickup_datetime AS STRING)) AS concated 
FROM tlc.combo5

# removing dupes
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY concated) row_number
  FROM tlc.combo6
)
WHERE row_number = 1
LIMIT 100

# attempt to check whether dupe removal was success; it was - returned zero records
SELECT COUNT(*), concated
FROM tlc.combo7
GROUP BY concated
HAVING COUNT(*) > 1 
LIMIT 1000;

# sanity check on unified table: do trips of greater distance cost more? - they do
SELECT AVG(CAST (total_amt AS FLOAT64)) FROM tlc.combo7
WHERE CAST (trip_distance AS FLOAT64) > 10

SELECT AVG(CAST (total_amt AS FLOAT64)) FROM tlc.combo7
WHERE CAST (trip_distance AS FLOAT64) < 10

## What share of rides are paid in cash? What share of rides paid in cash see tips?

# counting cash trips - 46% of trips are paid in cash
SELECT COUNT(*) FROM 
(SELECT payment_type FROM [sylvan-rampart-159916:tlc.combo7]
WHERE payment_type LIKE 'CSH') core; 

# counting cash trips with tips - 0.008% recorded tips
SELECT COUNT(*) FROM 
(SELECT payment_type FROM [sylvan-rampart-159916:tlc.combo7]
WHERE payment_type LIKE 'CSH' AND tip_amount > 0) core;

## What is the average tip amount in June 2013?

# with cash payments excluded
SELECT AVG(tip_amt) FROM [sylvan-rampart-159916:tlc.combo7]
WHERE MONTH(pickup_datetime) = 6
AND payment_type = 'CRD'

# getting avg tip share
SELECT AVG(tip_share) FROM
(SELECT tip_amt / total_amt AS tip_share FROM [sylvan-rampart-159916:tlc.combo7]
WHERE MONTH(pickup_datetime) = 6
AND payment_type = 'CRD') x

## Are any drivers dramatically improving their tip share from the first five months of 2013 to June?  

# getting ranked change of tip amount
SELECT hack_license, tip_avg_x, tip_avg_y, tip_avg_x - tip_avg_y AS tip_change FROM
(SELECT hack_license, AVG(tip_amt) AS tip_avg_x
FROM [sylvan-rampart-159916:tlc.combo7]
WHERE MONTH(pickup_datetime) BETWEEN 1 AND 5
AND payment_type = 'CRD'
GROUP BY hack_license) x
INNER JOIN
(SELECT hack_license AS hack_license2, AVG(tip_amt) AS tip_avg_y
FROM [sylvan-rampart-159916:tlc.combo7]
WHERE MONTH(pickup_datetime) = 6
AND payment_type = 'CRD'
GROUP BY hack_license2) y
ON x.hack_license = y.hack_license2
ORDER BY tip_change ASC 
LIMIT 100;

# following up on specific drivers to understand what's going on, some are just getting single massive tips
SELECT * FROM tlc.combo7
WHERE hack_license = 'C4D359A8759787127FE8FC3B206AFAE2'
AND MONTH(pickup_datetime) BETWEEN 1 AND 6
ORDER BY tip_amt DESC
LIMIT 100;

# introducing more aggressive outlier elimination to get at "normal" improvement
SELECT hack_license, tip_avg_x, tip_avg_y, tip_avg_x - tip_avg_y AS tip_change FROM
(SELECT hack_license, AVG(tip_amt) AS tip_avg_x
FROM [sylvan-rampart-159916:tlc.combo7]
WHERE MONTH(pickup_datetime) BETWEEN 1 AND 5
AND payment_type = 'CRD'
AND tip_amt <= 20
GROUP BY hack_license) x
INNER JOIN
(SELECT hack_license AS hack_license2, AVG(tip_amt) AS tip_avg_y
FROM [sylvan-rampart-159916:tlc.combo7]
WHERE MONTH(pickup_datetime) = 6
AND payment_type = 'CRD'
AND tip_amt <= 20
GROUP BY hack_license2) y
ON x.hack_license = y.hack_license2
ORDER BY tip_change ASC 
LIMIT 100;

# if you exclude outliers (rides with tips over $20),
# the driver with hack license 68F16B9B6A63E52C7FBE2E764A9AF872 has the largest change between periods – a $14.17 improvement

## Other analyses

# getting avg tip and avg tip share per day
SELECT DATE(pickup_datetime) AS ride_date, AVG (tip_amt) AS avg_tip, AVG (tip_amt / total_amt) AS tip_share
FROM tlc.combo7
WHERE payment_type = 'CRD'
AND total_amt != 0
GROUP BY DATE(pickup_datetime);

# avg tip and avg tip share per hour - this doesn't neatly map on to the regression analysis of the subset, though
SELECT HOUR(pickup_datetime) AS ride_hour, AVG (tip_amt) AS avg_tip, AVG (tip_amt / total_amt) AS tip_share
FROM tlc.combo7
WHERE payment_type = 'CRD'
AND total_amt != 0
GROUP BY ride_hour;
# 
