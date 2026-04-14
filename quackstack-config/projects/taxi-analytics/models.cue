package duckconfig

platform: projects: "taxi-analytics": {
	models: taxi_daily_summary: {
		materialization: "VIEW"
		description:     "Curated daily taxi summary model for seeded dashboard and semantic flows"
		tags: [
			"seeded",
			"taxi",
		]
		sql: """
			SELECT
			  service_date,
			  pickup_borough,
			  trip_count,
			  total_fare
			FROM seeded_local.nyc_taxi.trip_borough_metrics

			"""
	}
}
