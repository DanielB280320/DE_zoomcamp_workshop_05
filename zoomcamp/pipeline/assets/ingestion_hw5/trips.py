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
    # Get ingestion window from Bruin environment
    start_date = os.getenv("BRUIN_START_DATE", "2025-01-01")
    end_date = os.getenv("BRUIN_END_DATE", "2025-01-31")
    
    # Get taxi types from pipeline variables
    bruin_vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    
    extracted_at = datetime.utcnow()
    
    print(f"[INGESTION] Date range: {start_date} to {end_date} | Types: {taxi_types}")
    
    # Fetch and concatenate data for all taxi types
    dfs = [
        _fetch_data(taxi_type, start_date, end_date, extracted_at)
        for taxi_type in taxi_types
    ]
    dfs = [df for df in dfs if df is not None and len(df) > 0]
    
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        print(f"[SUCCESS] Ingested {len(result)} rows")
        return result
    else:
        print("[WARNING] No data found. Returning empty DataFrame.")
        return pd.DataFrame(columns=[
            "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
            "trip_distance", "rate_code_id", "store_and_fwd_flag", "pu_location_id",
            "do_location_id", "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "total_amount", "extracted_at"
        ])


def _fetch_data(taxi_type: str, start_date: str, end_date: str, extracted_at: datetime) -> pd.DataFrame:
    """Fetch taxi data from NYC TLC official dataset for a specific type and date range."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Generate list of (year, month) tuples for the date range
    year_months = _get_year_months(start, end)
    print(f"[{taxi_type.upper()}] Fetching {len(year_months)} month(s)...")
    
    dfs = []
    for year, month in year_months:
        try:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            df = _download_parquet(url)
            
            if df is not None:
                # Filter to date range
                df = _filter_by_date_range(df, start_date, end_date)
                if len(df) > 0:
                    dfs.append(df)
                    print(f"  {year}-{month:02d}: {len(df)} rows")
                    
        except Exception as e:
            print(f"  [WARNING] {year}-{month:02d}: {str(e)}")
    
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        result["extracted_at"] = extracted_at
        return result
    else:
        return None


def _get_year_months(start: datetime, end: datetime) -> list:
    """Generate list of (year, month) tuples for all months in date range."""
    year_months = []
    current = start.replace(day=1)
    
    while current <= end:
        year_months.append((current.year, current.month))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return year_months


def _download_parquet(url: str) -> pd.DataFrame:
    """Download and parse parquet file from URL."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    # Read parquet from BytesIO
    df = pd.read_parquet(BytesIO(response.content))
    return df


def _filter_by_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Filter DataFrame by pickup_datetime range."""
    # Ensure pickup_datetime is datetime
    if "pickup_datetime" in df.columns:
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)  # Include entire end day
        
        df = df[(df["pickup_datetime"] >= start) & (df["pickup_datetime"] < end)]
    
    return df