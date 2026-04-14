package duckconfig

platform: governance: {
	tags: {
		classification: [
			"pii",
			"sensitive",
			"confidential",
			"public",
			"personal_data",
		]
		contains_public_data: []
		dataset: ["demo"]
		sensitivity: [
			"high",
			"medium",
			"low",
		]
	}
	assignments: [{
		tag:            "dataset:demo"
		securable_type: "schema"
		securable:      "seeded_local.nyc_taxi"
	}, {
		tag:            "contains_public_data"
		securable_type: "schema"
		securable:      "seeded_local.nyc_taxi"
	}]
}
