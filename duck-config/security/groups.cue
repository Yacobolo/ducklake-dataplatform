package duckconfig

platform: security: groups: {
	"analytics-users": {
		name:        ""
		description: "Analysts and product owners exploring the seeded workspace"
		members: [{
			name: "analyst"
			type: "user"
		}, {
			name: "product_owner"
			type: "user"
		}]
	}
	"data-platform": {
		name:        ""
		description: "Engineering maintainers for the local seeded platform"
		members: [{
			name: "__DUCK_DEV_BOOTSTRAP_PRINCIPAL__"
			type: "user"
		}, {
			name: "data_eng"
			type: "user"
		}]
	}
}
