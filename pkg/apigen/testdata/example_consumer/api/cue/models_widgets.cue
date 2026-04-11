package api

schemas_widgets: {
	Widget: {
		type: "object"
		properties: {
			id: {
				schema: {
					type: "string"
				}
			}
			name: {
				schema: {
					type: "string"
				}
			}
		}
		required: ["id", "name"]
	}
	CreateWidgetRequest: {
		type: "object"
		properties: {
			name: {
				schema: {
					type: "string"
				}
			}
		}
		required: ["name"]
	}
	ListWidgetsResponse: {
		type: "object"
		properties: {
			data: {
				schema: {
					type: "array"
					items: {
						ref: "Widget"
					}
				}
			}
		}
		required: ["data"]
	}
	Error: {
		type: "object"
		properties: {
			code: {
				schema: {
					type: "integer"
					format: "int32"
				}
			}
			message: {
				schema: {
					type: "string"
				}
			}
		}
		required: ["code", "message"]
	}
}
