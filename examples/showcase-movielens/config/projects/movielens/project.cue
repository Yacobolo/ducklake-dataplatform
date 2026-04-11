package duckconfig

platform: projects: movielens: {
	workspace_ref: "showcase"
	kind:          "personal"
	description:   "MovieLens showcase analytics project"
	environments: dev: {
		kind:           "development"
		target_catalog: "lake"
		target_schema:  "main"
	}
	macros: rating_bucket: {
		macro_type: "SCALAR"
		parameters: ["rating_value"]
		body: """
			CASE
			  WHEN rating_value >= 4.5 THEN 'excellent'
			  WHEN rating_value >= 3.5 THEN 'good'
			  WHEN rating_value >= 2.5 THEN 'mixed'
			  ELSE 'low'
			END
			"""
		description: "Bucket ratings into stable quality bands"
		visibility:  "project"
		status:      "ACTIVE"
		tags: [
			"movielens",
			"quality",
		]
	}
	models: {
		bronze_movies: {
			materialization: "VIEW"
			description:     "Raw MovieLens movies loaded through ingestion API"
			tags: ["bronze", "movielens"]
			sql: """
				SELECT
				  movie_id,
				  title,
				  genres
				FROM lake.main.raw_movies
				"""
		}
		bronze_ratings: {
			materialization: "VIEW"
			description:     "Raw MovieLens ratings loaded through ingestion API"
			tags: ["bronze", "movielens"]
			sql: """
				SELECT
				  user_id,
				  movie_id,
				  rating,
				  rating_ts
				FROM lake.main.raw_ratings
				"""
		}
		bronze_users: {
			materialization: "VIEW"
			description:     "Raw user demographic attributes loaded through ingestion API"
			tags: ["bronze", "movielens"]
			sql: """
				SELECT
				  user_id,
				  age_group,
				  region
				FROM lake.main.raw_users
				"""
		}
		silver_movies: {
			materialization: "TABLE"
			description:     "Conformed movie dimension with release year extraction"
			tags: ["silver", "movielens"]
			sql: """
				SELECT
				  movie_id,
				  title,
				  genres,
				  regexp_extract(title, '\\((\\d{4})\\)$', 1) AS release_year
				FROM lake.main.bronze_movies
				"""
		}
		silver_ratings_enriched: {
			materialization: "TABLE"
			description:     "Ratings enriched with user and movie attributes"
			tags: ["silver", "movielens"]
			sql: """
				SELECT
				  r.user_id,
				  r.movie_id,
				  r.rating,
				  r.rating_ts,
				  u.region,
				  u.age_group,
				  m.genres,
				  {{ rating_bucket(r.rating) }} AS rating_bucket
				FROM lake.main.bronze_ratings r
				JOIN lake.main.silver_users u ON u.user_id = r.user_id
				JOIN lake.main.silver_movies m ON m.movie_id = r.movie_id
				"""
		}
		silver_users: {
			materialization: "TABLE"
			description:     "Conformed user dimension"
			tags: ["silver", "movielens"]
			sql: """
				SELECT
				  user_id,
				  age_group,
				  upper(region) AS region
				FROM lake.main.bronze_users
				"""
		}
		gold_movie_scores: {
			materialization: "TABLE"
			description:     "Gold aggregate of movie quality metrics"
			tags: ["gold", "movielens"]
			sql: """
				SELECT
				  movie_id,
				  COUNT(*) AS rating_count,
				  ROUND(AVG(rating), 3) AS avg_rating,
				  SUM(CASE WHEN rating >= 4.0 THEN 1 ELSE 0 END) AS positive_ratings
				FROM lake.main.silver_ratings_enriched
				GROUP BY movie_id
				"""
		}
		gold_user_engagement: {
			materialization: "VIEW"
			description:     "Gold aggregate of user activity and quality preference"
			tags: ["gold", "movielens"]
			freshness: {
				max_lag_seconds: 3600
				cron_schedule:   "*/30 * * * *"
			}
			sql: """
				SELECT
				  user_id,
				  region,
				  COUNT(*) AS ratings_submitted,
				  ROUND(AVG(rating), 3) AS avg_rating_given,
				  SUM(CASE WHEN rating_bucket = 'excellent' THEN 1 ELSE 0 END) AS excellent_votes
				FROM lake.main.silver_ratings_enriched
				GROUP BY user_id, region
				"""
		}
	}
}
