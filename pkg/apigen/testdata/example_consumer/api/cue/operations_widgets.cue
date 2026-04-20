package api

endpoints_widgets: [
	{
		method:       "get"
		path:         "/widgets"
		operation_id: "listWidgets"
		summary:      "List widgets"
		tags:         ["Widgets"]
		responses: [
			{
				status_code: 200
				description: "The request has succeeded."
				schema: {
					ref: "ListWidgetsResponse"
				}
				extensions: {
					"x-apigen-response-shape": {
						kind:      "wrapped_json"
						body_type: "ListWidgetsResponse"
					}
				}
			},
			{
				status_code: 401
				description: "Access is unauthorized."
				schema: {
					ref: "Error"
				}
				extensions: {
					"x-apigen-response-shape": {
						kind:      "wrapped_json"
						body_type: "ListWidgetsResponse"
					}
				}
			},
		]
		cli: {
			command: ["widgets", "list"]
			output: {
				mode: "collection"
				table_columns: ["id", "name"]
				quiet_fields: ["id", "name"]
			}
			pagination: {
				items_field: "data"
				next_page_token_field: "next_page_token"
			}
		}
	},
	{
		method:       "post"
		path:         "/widgets"
		operation_id: "createWidget"
		summary:      "Create widget"
		tags:         ["Widgets"]
		request_body: {
			required: true
			schema: {
				ref: "CreateWidgetRequest"
			}
		}
		responses: [
			{
				status_code: 201
				description: "The request has succeeded and a new resource has been created as a result."
				schema: {
					ref: "Widget"
				}
				extensions: {
					"x-apigen-response-shape": {
						kind:      "wrapped_json"
						body_type: "Widget"
					}
				}
			},
			{
				status_code: 401
				description: "Access is unauthorized."
				schema: {
					ref: "Error"
				}
				extensions: {
					"x-apigen-response-shape": {
						kind:      "wrapped_json"
						body_type: "Widget"
					}
				}
			},
		]
		cli: {
			command: ["widgets", "create"]
			body_input: "flags_or_json"
			args: [{
				source: "body"
				name: "name"
				display_name: "name"
			}]
			output: {
				mode: "detail"
				quiet_fields: ["id", "name"]
			}
		}
	},
	{
		method:       "delete"
		path:         "/widgets/{widget_id}"
		operation_id: "deleteWidget"
		summary:      "Delete widget"
		tags:         ["Widgets"]
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
				description: "The request has succeeded and there is no additional content to send in the response payload body."
			},
		]
		extensions: {
			"x-apigen-manual": true
		}
	},
]
