package duckconfig

platform: data_products: "movielens-analytics": {
	name:              "MovieLens Analytics"
	description:       "Product contract for the MovieLens showcase KPI outputs"
	domain_ref:        "analytics"
	owner_team_ref:    "data-eng"
	steward_principal: "data_eng"
	contact_channel:   "#data-eng"
	consumer_audience: "internal-analytics"
	contract: {
		data_grain:     "one notebook-driven KPI snapshot per daily showcase run"
		update_cadence: "daily"
		sample_queries: ["open notebook 01_kpi_walkthrough"]
	}
	slo: freshness_slo: "24h"
	outputs: ["movielens_daily"]
	publication_intent: "draft"
}
