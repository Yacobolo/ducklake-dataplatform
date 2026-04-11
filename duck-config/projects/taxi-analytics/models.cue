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
	semantic_models: taxi_daily_summary: {
		description:            "Semantic metrics over the seeded taxi daily summary model"
		base_model_ref:         "taxi_daily_summary"
		default_time_dimension: "service_date"
		tags: [
			"seeded",
			"semantic",
		]
		metrics: [{
			name:            "total_fare"
			metric_type:     "SUM"
			expression_mode: "DSL"
			expression:      "total_fare"
		}, {
			name:            "trip_count"
			metric_type:     "SUM"
			expression_mode: "DSL"
			expression:      "trip_count"
		}]
	}
}
