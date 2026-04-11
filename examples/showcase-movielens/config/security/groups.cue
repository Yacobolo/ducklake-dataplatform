package duckconfig

platform: security: groups: movie_analysts: {
	name:        ""
	description: "Analysts with read access to MovieLens source tables"
	members: [{
		name: "ml_analyst"
		type: "user"
	}]
}
