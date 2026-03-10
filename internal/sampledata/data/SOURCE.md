NYC Taxi sample data provenance

- Official trip source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
- Official zone source: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- Dataset owner: NYC Taxi & Limousine Commission (TLC)
- Landing page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Bundled subset details

- File: `nyc_taxi_trips.parquet`
- Source month: January 2024 yellow taxi trips
- Shape: deterministic curated subset, capped at 8,000 trips per pickup day
- Included rows: 248,000 trips
- Included columns: vendor, pickup/dropoff timestamps, passenger count, trip distance, pickup/dropoff locations, payment type, and fare/tip/total metrics

Generation query

```sql
COPY (
WITH src AS (
  SELECT
    CAST(VendorID AS INTEGER) AS vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_at,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_at,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS DOUBLE) AS trip_distance_mi,
    CAST(RatecodeID AS INTEGER) AS ratecode_id,
    CAST(store_and_fwd_flag AS VARCHAR) AS store_and_fwd_flag,
    CAST(PULocationID AS INTEGER) AS pickup_location_id,
    CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
    CAST(payment_type AS INTEGER) AS payment_type,
    CAST(fare_amount AS DOUBLE) AS fare_amount,
    CAST(extra AS DOUBLE) AS extra,
    CAST(mta_tax AS DOUBLE) AS mta_tax,
    CAST(tip_amount AS DOUBLE) AS tip_amount,
    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    CAST(total_amount AS DOUBLE) AS total_amount,
    CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    CAST(airport_fee AS DOUBLE) AS airport_fee
  FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')
  WHERE tpep_pickup_datetime >= TIMESTAMP '2024-01-01'
    AND tpep_pickup_datetime < TIMESTAMP '2024-02-01'
    AND trip_distance > 0
    AND total_amount > 0
    AND PULocationID IS NOT NULL
    AND DOLocationID IS NOT NULL
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY DATE_TRUNC('day', pickup_at)
           ORDER BY pickup_at, vendor_id, pickup_location_id, dropoff_location_id, total_amount DESC
         ) AS rn
  FROM src
)
SELECT * EXCLUDE (rn)
FROM ranked
WHERE rn <= 8000
) TO 'nyc_taxi_trips.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```
