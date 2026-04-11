package duckconfig

platform: catalogs: seeded_local: {
	metastore_type: "sqlite"
	dsn:            "__DUCK_DEV_SAMPLE_METASTORE__"
	data_path:      "__DUCK_DEV_SAMPLE_DATA_DIR__"
	comment:        "Local seeded NYC taxi catalog for developer workflows"
	schemas: {
		main: {}
		nyc_taxi: {
			comment: "Public NYC TLC taxi data cached locally for development"
			owner:   "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
			views: {
				raw_trips: {
					view_definition: """
						SELECT
						  CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_at,
						  CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_at,
						  CAST(PULocationID AS INTEGER) AS pickup_location_id,
						  CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
						  CAST(passenger_count AS INTEGER) AS passenger_count,
						  CAST(trip_distance AS DOUBLE) AS trip_distance,
						  CAST(total_amount AS DOUBLE) AS total_amount,
						  CAST(fare_amount AS DOUBLE) AS fare_amount,
						  CAST(tip_amount AS DOUBLE) AS tip_amount
						FROM read_parquet('__DUCK_DEV_TAXI_TRIPS__')

						"""
					comment: "Direct view over the cached public TLC parquet file"
					owner:   "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
				}
				taxi_zones: {
					view_definition: """
						SELECT
						  CAST(LocationID AS INTEGER) AS location_id,
						  Borough AS borough,
						  Zone AS zone,
						  service_zone
						FROM read_csv_auto('__DUCK_DEV_TAXI_ZONES__', header = true)

						"""
					comment: "TLC taxi zone lookup cached from the public CSV"
					owner:   "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
				}
				trip_borough_metrics: {
					view_definition: """
						SELECT
						  CAST(r.pickup_at AS DATE) AS service_date,
						  COALESCE(z.borough, 'Unknown') AS pickup_borough,
						  COUNT(*) AS trip_count,
						  ROUND(SUM(r.total_amount), 2) AS total_fare
						FROM nyc_taxi.raw_trips AS r
						LEFT JOIN nyc_taxi.taxi_zones AS z
						  ON z.location_id = r.pickup_location_id
						GROUP BY 1, 2

						"""
					comment: "Borough-level trip and fare metrics for dashboard slices"
					owner:   "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
				}
				trip_daily_metrics: {
					view_definition: """
						SELECT
						  CAST(pickup_at AS DATE) AS service_date,
						  COUNT(*) AS trip_count,
						  ROUND(SUM(total_amount), 2) AS total_fare,
						  ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
						  ROUND(AVG(passenger_count), 2) AS avg_passengers
						FROM nyc_taxi.raw_trips
						GROUP BY 1

						"""
					comment: "Daily trip and fare metrics derived from the public NYC taxi seed"
					owner:   "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
				}
			}
		}
	}
}
