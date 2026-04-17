package api

schema_version: "v1"

api: {
	base_path: "/v1"
}

info: {
	title:   "Widget API"
	version: "0.1.0"
}

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

tags: [
	{
		name:        "Widgets"
		description: "Widget lifecycle operations."
	},
]

schemas: {
	"Widget": {
		type: "object"
		properties: {
			"id": {
				schema: {
					type: "string"
				}
			}
			"name": {
				schema: {
					type: "string"
				}
			}
		}
		required: ["id", "name"]
	}
	"Error": {
		type: "object"
		properties: {
			"message": {
				schema: {
					type: "string"
				}
			}
		}
		required: ["message"]
	}
}

endpoints: [
	{
		method:       "get"
		path:         "/widgets"
		operation_id: "listWidgets"
		summary:      "List widgets"
		tags:         ["Widgets"]
		responses: [
			{
				status_code: 200
				description: "ok"
				schema: {
					ref: "Widget"
				}
			},
			{
				status_code: 401
				description: "unauthorized"
				schema: {
					ref: "Error"
				}
			},
		]
		cli: {
			command: ["widgets", "list"]
		}
		extensions: {
			"x-authz": {
				mode: "authenticated"
			}
		}
	},
	{
		method:       "delete"
		path:         "/widgets/{widget_id}"
		operation_id: "deleteWidget"
		summary:      "Delete widget"
		parameters: [
			{
				name:     "widget_id"
				in:       "path"
				required: true
				schema: {
					type: "string"
				}
			},
		]
		responses: [
			{
				status_code: 204
				description: "deleted"
			},
			{
				status_code: 404
				description: "not found"
				schema: {
					ref: "Error"
				}
			},
		]
		extensions: {
			"x-apigen-manual": true
		}
	},
]
