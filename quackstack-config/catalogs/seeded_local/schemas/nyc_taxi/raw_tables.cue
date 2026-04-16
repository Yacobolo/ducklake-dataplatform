package duckconfig

platform: catalogs: seeded_local: schemas: nyc_taxi: tables: {
	trips: {
		table_type: "MANAGED"
		columns: [
			{
				name: "pickup_at"
				type: "TIMESTAMP"
			},
			{
				name: "dropoff_at"
				type: "TIMESTAMP"
			},
			{
				name: "pickup_location_id"
				type: "INTEGER"
			},
			{
				name: "dropoff_location_id"
				type: "INTEGER"
			},
			{
				name: "passenger_count"
				type: "INTEGER"
			},
			{
				name: "trip_distance"
				type: "DOUBLE"
			},
			{
				name: "total_amount"
				type: "DOUBLE"
			},
			{
				name: "fare_amount"
				type: "DOUBLE"
			},
			{
				name: "tip_amount"
				type: "DOUBLE"
			},
		]
		comment: "Managed local copy of the cached public TLC parquet seed"
		owner:   #BootstrapPrincipal
	}
	zones: {
		table_type: "MANAGED"
		columns: [
			{
				name: "location_id"
				type: "INTEGER"
			},
			{
				name: "borough"
				type: "VARCHAR"
			},
			{
				name: "zone"
				type: "VARCHAR"
			},
			{
				name: "service_zone"
				type: "VARCHAR"
			},
		]
		comment: "Managed local copy of the TLC taxi zone lookup seed"
		owner:   #BootstrapPrincipal
	}
}
