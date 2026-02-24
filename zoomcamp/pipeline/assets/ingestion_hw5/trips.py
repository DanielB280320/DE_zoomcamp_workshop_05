"""@bruin

name: ingestion_hw5.trips
type: python
image: python:3.11
connection: gcp-default

materialization:
  type: table
  strategy: append

# columns:
#   - name: vendor_id
#     type: INT
#     description: TPEP provider that provided the record
#   - name: pickup_datetime
#     type: TIMESTAMP
#     description: Date and time when the meter was engaged
#     primary_key: true
#   - name: dropoff_datetime
#     type: TIMESTAMP
#     description: Date and time when the meter was disengaged
#   - name: passenger_count
#     type: INT
#     description: Number of passengers in the vehicle
#   - name: trip_distance
#     type: NUMERIC
#     description: Elapsed trip distance in miles
#   - name: rate_code_id
#     type: INT
#     description: Final rate code in effect at the end of the trip
#   - name: store_and_fwd_flag
#     type: STRING
#     description: Flag indicating if trip record was held in vehicle memory
#   - name: pu_location_id
#     type: INT
#     description: TLC Taxi Zone ID for pickup location
#   - name: do_location_id
#     type: INT
#     description: TLC Taxi Zone ID for dropoff location
#   - name: payment_type
#     type: INT
#     description: Numeric code signifying how the passenger paid
#   - name: fare_amount
#     type: NUMERIC
#     description: Time-and-distance fare charged by the meter
#   - name: extra
#     type: NUMERIC
#     description: Miscellaneous extras and surcharges
#   - name: mta_tax
#     type: NUMERIC
#     description: MTA tax charge
#   - name: tip_amount
#     type: NUMERIC
#     description: Tip amount for the trip
#   - name: tolls_amount
#     type: NUMERIC
#     description: Total amount of all tolls paid in trip
#   - name: total_amount
#     type: NUMERIC
#     description: Total amount charged to the passenger
#   - name: extracted_at
#     type: TIMESTAMP
#     description: Timestamp when the record was extracted

@bruin"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import BytesIO


def materialize():
    """
    Ingest NYC taxi trip data from official TLC dataset (parquet files).
    
    Fetches data from: https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet
    Returns DataFrame for Bruin to load into destination.
    """

    def generate_year_months(start_date, end_date):
        year_months = []
        current = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        while current <= end_datetime:
            year_months.append((current.year, current.month))
            current = current.replace(month=1, year=current.year + 1) if current.month == 12 else current.replace(month=current.month + 1)
        return year_months

    def fetch_taxi_data(taxi_type, year, month, start_date, end_date):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
        print(f"Downloading {taxi_type} {year}-{month:02d}", end=" ")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        df = pd.read_parquet(BytesIO(response.content))
        print(f"({len(df)} rows before filtering)")

        if "pickup_datetime" in df.columns:
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            filter_start = datetime.strptime(start_date, "%Y-%m-%d")
            filter_end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            df = df[(df["pickup_datetime"] >= filter_start) & (df["pickup_datetime"] < filter_end)]

        print(f"After date filtering: {len(df)} rows")
        return df if len(df) > 0 else None

    # Load configuration
    print("Loading configuration")
    start_date = os.getenv("BRUIN_START_DATE", "2025-01-01")
    end_date = os.getenv("BRUIN_END_DATE", "2025-01-31")
    extraction_timestamp = datetime.utcnow()
    bruin_vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    print(f" - Date range: {start_date} to {end_date}")
    print(f" - Taxi types: {taxi_types}")
    print(f" - Extraction timestamp: {extraction_timestamp}")

    # Generate year-month range
    print("\n Generating year-month list")
    year_months = generate_year_months(start_date, end_date)
    print(f"- Generated {len(year_months)} month(s): {year_months}")

    # Download and process data for all taxi types
    print("\n Downloading and processing data")
    all_dataframes = []
    for taxi_type in taxi_types:
        print(f"\n Taxi type: {taxi_type}")
        taxi_type_dataframes = []
        for year, month in year_months:
            try:
                df = fetch_taxi_data(taxi_type, year, month, start_date, end_date)
                if df is not None:
                    taxi_type_dataframes.append(df)
            except Exception as e:
                print(f"Error: {str(e)}")

        if taxi_type_dataframes:
            taxi_type_df = pd.concat(taxi_type_dataframes, ignore_index=True)
            taxi_type_df["extracted_at"] = extraction_timestamp
            all_dataframes.append(taxi_type_df)
            print(f"Combined {taxi_type}: {len(taxi_type_df)} rows")
        else:
            print(f"No data found for {taxi_type}")

    # Validate and return final result
    print("\n Finalizing results")
    valid_dataframes = [df for df in all_dataframes if df is not None and len(df) > 0]
    if valid_dataframes:
        final_result = pd.concat(valid_dataframes, ignore_index=True)
        print(f"Total rows ingested: {len(final_result)}")
        return final_result

    print("No data found")
    return pd.DataFrame(columns=[
        "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
        "trip_distance", "rate_code_id", "store_and_fwd_flag", "pu_location_id",
        "do_location_id", "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "total_amount", "extracted_at"
    ])