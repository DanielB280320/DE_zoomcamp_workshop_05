/* @bruin

name: staging_hw5.trips
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

depends:
  - ingestion_hw5.trips
  - ingestion_hw5.payment_lookup
  - ingestion_hw5.taxi_zone_lookup

columns:
  - name: vendor_id
    type: INTEGER
    Description: The ID of the taxi vendor.
    checks:
      - name: not_null
  - name: tpep_pickup_datetime
    type: TIMESTAMP
    Description: The date and time when the trip started.
    checks:
      - name: not_null
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the trip ended.
    checks:
      - name: not_null
  - name: passenger_count
    type: NUMERIC
    description: The number of passengers in the trip.
  - name: trip_distance
    type: NUMERIC
    description: The distance of the trip in miles.
  - name: ratecode_id
    type: NUMERIC
    description: The ID of the rate code for the trip.
  - name: store_and_fwd_flag
    type: STRING
  - name: pu_location_id
    type: INTEGER
    description: The ID of the pickup location (taxi zone).
    checks:
      - name: not_null
  - name: do_location_id
    type: INTEGER
    description: The ID of the dropoff location (taxi zone).
    checks:
      - name: not_null
  - name: payment_type
    type: INTEGER
    description: The ID of the payment type for the trip.
  - name: fare_amount
    type: NUMERIC
    description: The fare amount for the trip.
  - name: extra
    type: NUMERIC
    description: The extra charges for the trip (e.g., airport surcharge).
  - name: mta_tax
    type: NUMERIC
    description: The MTA tax for the trip.
  - name: tip_amount
    type: NUMERIC
    description: The tip amount for the trip.
  - name: tolls_amount
    type: NUMERIC
    description: The tolls amount for the trip.
  - name: improvement_surcharge
    type: NUMERIC
    description: The improvement surcharge for the trip.
  - name: total_amount
    type: NUMERIC
    description: The total amount for the trip (fare + extra + tax + tip + tolls + surcharge).
  - name: congestion_surcharge
    type: NUMERIC
    description: The congestion surcharge for the trip.
  - name: airport_fee
    type: NUMERIC
    description: The airport fee for the trip.
  - name: cbd_congestion_fee
    type: NUMERIC
    description: The CBD congestion fee for the trip.
  - name: extracted_at
    type: TIMESTAMP
    description: The timestamp when the trip record was extracted from the source.

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH cleaned_trips AS (
SELECT 
  vendor_id,
  tpep_pickup_datetime AS pickup_datetime,
  tpep_dropoff_datetime AS dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pu_location_id,
  do_location_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  cbd_congestion_fee,
  extracted_at

FROM ingestion_hw5.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
  AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) > 0 -- filter trips with negative or zero duration
  AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, HOUR) < 24 -- filter trips longer than 24 hours (possible outliers)
  AND passenger_count IS NOT NULL AND passenger_count > 0 -- filter out trips with null or zero passengers
  AND total_amount IS NOT NULL AND total_amount >= 0 -- filter out trips with null or negative total amount
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY 
    vendor_id,
    tpep_pickup_datetime, 
    tpep_dropoff_datetime, 
    pu_location_id,
    do_location_id
  ORDER BY 
    tpep_pickup_datetime
) = 1 -- deduplicate trips with same pickup/dropoff time and location, keep the earliest record
ORDER BY tpep_pickup_datetime DESC
), 

enriched_trips AS (
SELECT 
  ct.vendor_id,
  ct.pickup_datetime,
  ct.dropoff_datetime,
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_minutes,
  ct.passenger_count,
  ct.trip_distance,
  ct.ratecode_id,
  ct.store_and_fwd_flag,
  ct.pu_location_id,
  pu.zone AS pickup_zone,
  pu.borough AS pickup_borough,
  ct.do_location_id,
  do.zone AS dropoff_zone,
  do.borough AS dropoff_borough, 
  ct.payment_type,
  pl.payment_type_name,
  ct.fare_amount,
  ct.extra,
  ct.mta_tax,
  ct.tip_amount,
  ct.tolls_amount,
  ct.improvement_surcharge,
  ct.total_amount,
  ct.congestion_surcharge,
  ct.airport_fee,
  ct.cbd_congestion_fee,
  ct.extracted_at

FROM cleaned_trips ct
LEFT JOIN ingestion_hw5.payment_lookup pl
  ON ct.payment_type = pl.payment_type_id

LEFT JOIN ingestion_hw5.taxi_zone_lookup pu
  ON ct.pu_location_id = pu.locationid
  
LEFT JOIN ingestion_hw5.taxi_zone_lookup do
  ON ct.do_location_id = do.locationid
)

SELECT * 
FROM enriched_trips




