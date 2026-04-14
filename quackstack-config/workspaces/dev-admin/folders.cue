package duckconfig

platform: workspaces: "dev-admin": folders: analytics: {
	default_project_ref:     #TaxiProjectRef
	default_environment_ref: #TaxiEnvironmentRef
	folders: explore: {
		default_project_ref:     #TaxiProjectRef
		default_environment_ref: #TaxiEnvironmentRef
		notebooks: nyc_taxi_explore: {
			description:     "Exploration notebook for the seeded NYC taxi local dev dataset"
			owner:           #BootstrapPrincipal
			project_ref:     #TaxiProjectRef
			environment_ref: #TaxiEnvironmentRef
			cells: [{
				type: "markdown"
				content: """
					# NYC Taxi Explore

					This notebook is part of the default local dev seed and points at the
					cached public TLC taxi data.

					"""
			}, {
				type: "sql"
				name: "recent_daily_trips"
				role: "output"
				content: """
					SELECT
					  service_date,
					  trip_count,
					  total_fare,
					  avg_trip_distance
					FROM seeded_local.nyc_taxi.trip_daily_metrics
					ORDER BY service_date DESC
					LIMIT 30

					"""
			}, {
				type: "sql"
				name: "borough_fares"
				content: """
					SELECT
					  pickup_borough,
					  SUM(trip_count) AS trip_count,
					  ROUND(SUM(total_fare), 2) AS total_fare
					FROM seeded_local.nyc_taxi.trip_borough_metrics
					GROUP BY pickup_borough
					ORDER BY total_fare DESC

					"""
			}]
		}
	}
}
