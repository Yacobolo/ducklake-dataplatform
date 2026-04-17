package api

schema_version: "v1"

api: {
	base_path: "/"
}

info: {
	title:       "APIGen Example API"
	version:     "0.1.0"
	description: "Minimal example API authored in CUE for APIGen smoke tests."
}

servers: [
	{
		url:         "http://127.0.0.1:8081"
		description: "Example development server"
		variables:   {}
	},
]

tags: [
	{
		name:        "Widgets"
		description: "Example widget lifecycle endpoints."
	},
]

openapi: {
	version: "3.0.0"
	tag_order: ["Widgets"]
	security_schemes: {
		BearerAuth: {
			type:   "http"
			scheme: "Bearer"
		}
		ApiKeyAuth: {
			type: "apiKey"
			in:   "header"
			name: "X-API-Key"
		}
	}
}
