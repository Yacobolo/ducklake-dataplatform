package duckconfig

platform: catalogs: seeded_local: schemas: nyc_taxi: views: {
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
			FROM read_parquet('\(#TripsDatasetPath)')

			"""
		comment: "Direct view over the cached public TLC parquet file"
		owner:   #BootstrapPrincipal
	}
	taxi_zones: {
		view_definition: """
			SELECT
			  CAST(LocationID AS INTEGER) AS location_id,
			  Borough AS borough,
			  Zone AS zone,
			  service_zone
			FROM read_csv_auto('\(#ZonesDatasetPath)', header = true)

			"""
		comment: "TLC taxi zone lookup cached from the public CSV"
		owner:   #BootstrapPrincipal
	}
}
