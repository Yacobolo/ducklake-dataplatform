package duckconfig

platform: projects: "taxi-analytics": {
	workspace_ref:  "dev-admin"
	kind:           "personal"
	description:    "Seeded analytics authoring project for local development"
	default_branch: "main"
	environments: dev: {
		project_ref:      ""
		kind:             "development"
		description:      "Local development environment bound to the seeded taxi catalog"
		target_catalog:   "seeded_local"
		target_schema:    "nyc_taxi"
		compute_endpoint: "local-dev"
	}
	macros: safe_divide: {
		macro_type: "SCALAR"
		parameters: [
			"numerator",
			"denominator",
		]
		body:        "CASE WHEN denominator = 0 THEN NULL ELSE numerator / denominator END"
		description: "Avoid divide-by-zero errors in quick seeded analyses"
		owner:       "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
	}
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
