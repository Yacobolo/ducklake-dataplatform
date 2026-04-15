package duckconfig

platform: workspaces: "dev-admin": {
	semantic_models: taxi_daily_summary: {
		description:            "Semantic metrics over the seeded taxi daily summary model"
		base_relation_ref:      "taxi_daily_summary"
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
