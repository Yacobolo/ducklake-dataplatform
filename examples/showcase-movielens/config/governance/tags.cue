package duckconfig

platform: governance: {
	tags: {
		domain: ["media_analytics"]
		sensitivity: ["high"]
	}
	assignments: [{
		tag:            "domain:media_analytics"
		securable_type: "table"
		securable:      "lake.main.raw_movies"
	}, {
		tag:            "sensitivity:high"
		securable_type: "table"
		securable:      "lake.main.raw_users"
	}]
}
