package duckconfig

platform: workspaces: "dev-admin": dashboards: "nyc-taxi-ops": {
	description:           "Seeded local dashboard for NYC taxi exploration"
	owner:                 #BootstrapPrincipal
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
