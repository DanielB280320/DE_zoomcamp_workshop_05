/* @bruin

name: reports_hw5.trips_report
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - staging_hw5.trips

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  DATE_TRUNC(pickup_datetime, MONTH) AS pickup_month,
  pickup_zone,
  COUNT(*) AS total_trips,
  SUM(trip_duration_minutes) AS total_trip_duration_minutes,
  SUM(passenger_count) AS total_passengers,
  SUM(trip_distance) AS total_trip_distance,
  SUM(fare_amount) AS total_fare_amount,
  SUM(tip_amount) AS total_tip_amount,
  SUM(tolls_amount) AS total_tolls_amount,
  SUM(total_amount) AS total_total_amount, 
  SUM(airport_fee) AS total_airport_fee, 

  AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
  AVG(passenger_count) AS avg_passenger_count,
  AVG(trip_distance) AS avg_trip_distance,
  AVG(total_amount) AS avg_total_amount, 
  MAX(tip_amount) AS max_tip_amount,
  MAX(total_amount) AS max_total_amount

FROM staging_hw5.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 
  DATE_TRUNC(pickup_datetime, MONTH),
  pickup_zone
ORDER BY pickup_month DESC

;
