package duckconfig

platform: workspaces: "dev-admin": {
	kind:                    "personal"
	owner_principal:         "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
	default_project_ref:     "dev-admin/taxi-analytics"
	default_environment_ref: "dev-admin/taxi-analytics/dev"
	folders: analytics: {
		default_project_ref:     "dev-admin/taxi-analytics"
		default_environment_ref: "dev-admin/taxi-analytics/dev"
		folders: explore: {
			default_project_ref:     "dev-admin/taxi-analytics"
			default_environment_ref: "dev-admin/taxi-analytics/dev"
			notebooks: nyc_taxi_explore: {
				description:     "Exploration notebook for the seeded NYC taxi local dev dataset"
				owner:           "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
				project_ref:     "dev-admin/taxi-analytics"
				environment_ref: "dev-admin/taxi-analytics/dev"
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
	dashboards: "nyc-taxi-ops": {
		description:           "Seeded local dashboard for NYC taxi exploration"
		owner:                 "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
		semantic_project_name: "taxi-analytics"
		semantic_model_name:   "taxi_daily_summary"
		compute: mode: "AUTO"
		widgets: [{
			key:       "recent-daily-trips"
			page_name: "Overview"
			name:      "Recent Daily Trips"
			source: {
				kind: "notebook_cell"
				notebook_cell: {
					notebook_name: "nyc_taxi_explore"
					cell_name:     "recent_daily_trips"
				}
			}
			layout: {
				x: 0
				y: 0
				w: 6
				h: 4
			}
		}, {
			key:       "borough-fares"
			page_name: "Overview"
			name:      "Borough Fare Mix"
			source: {
				kind: "semantic_query"
				semantic_query: {
					metrics: ["total_fare"]
					dimensions: ["pickup_borough"]
				}
			}
			layout: {
				x: 6
				y: 0
				w: 6
				h: 4
			}
		}]
	}
}
