package duckconfig

platform: catalogs: lake: {
	metastore_type: "sqlite"
	dsn:            "./ducklake_meta.sqlite"
	data_path:      "./ducklake_data/"
	schemas: main: tables: {
		raw_movies: {
			table_type: "MANAGED"
			comment:    "Raw movie dimension loaded through ingestion API"
			owner:      "ml_admin"
			columns: [{
				name: "movie_id"
				type: "int64"
			}, {
				name: "title"
				type: "varchar"
			}, {
				name: "genres"
				type: "varchar"
			}]
		}
		raw_ratings: {
			table_type: "MANAGED"
			comment:    "Raw ratings fact loaded through ingestion API"
			owner:      "ml_admin"
			columns: [{
				name: "user_id"
				type: "int64"
			}, {
				name: "movie_id"
				type: "int64"
			}, {
				name: "rating"
				type: "float64"
			}, {
				name: "rating_ts"
				type: "int64"
			}]
		}
		raw_users: {
			table_type: "MANAGED"
			comment:    "Raw user dimension with policy-controlled access"
			owner:      "ml_admin"
			columns: [{
				name: "user_id"
				type: "int64"
			}, {
				name: "age_group"
				type: "varchar"
			}, {
				name: "region"
				type: "varchar"
			}]
		}
	}
}
