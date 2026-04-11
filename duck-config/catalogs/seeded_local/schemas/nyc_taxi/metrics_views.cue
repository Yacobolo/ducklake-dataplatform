package duckconfig

platform: catalogs: seeded_local: schemas: nyc_taxi: views: {
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
		owner:   #BootstrapPrincipal
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
		owner:   #BootstrapPrincipal
	}
}
