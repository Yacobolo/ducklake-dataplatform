package api

// Authored dashboard operations.

#dashboardsTag: "Dashboards"

#plainDashboardOperation: #genericOperationSpec & {
	wrapped: false
}

#dashboardIDPathParameter: #pathStringParameter & {
	#name: "dashboard_id"
}

#widgetIDPathParameter: #pathStringParameter & {
	#name: "widget_id"
}

#dashboardOwnerQueryParameter: #queryStringParameter & {
	#name: "owner"
}

#dashboardFiltersQueryParameter: {
	name:    "filters"
	in:      "query"
	explode: false
	schema: {
		type: "array"
		items: {
			type: "string"
		}
	}
}

#dashboardPathParameters: [
	#dashboardIDPathParameter,
]

#renderedDashboardParameters: [
	#dashboardIDPathParameter,
	#dashboardFiltersQueryParameter,
]

#dashboardWidgetPathParameters: [
	#dashboardIDPathParameter,
	#widgetIDPathParameter,
]

#listDashboardParameters: [
	#dashboardOwnerQueryParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#dashboardOps: [
	#plainDashboardOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDashboards"
		path:         "/dashboards"
		summary:      "List dashboards"
		cli: {
			command: ["dashboards", "list"]
		}
		returns:      "PaginatedDashboards"
		error_family: "standard"
		params:       #listDashboardParameters
	},
	#plainDashboardOperation & {
		kind:           "response"
		method:         "post"
		op:             "createDashboard"
		path:           "/dashboards"
		summary:        "Create dashboard"
		cli: {
			command: ["dashboards", "create"]
		}
		returns:        "Dashboard"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreateDashboardRequest"
		body_description: "Request payload"
	},
	#plainDashboardOperation & {
		kind:         "response"
		method:       "get"
		op:           "getDashboard"
		path:         "/dashboards/{dashboard_id}"
		summary:      "Get dashboard"
		cli: {
			command: ["dashboards", "get"]
		}
		returns:      "DashboardDetail"
		error_family: "resource"
		params:       #dashboardPathParameters
	},
	#plainDashboardOperation & {
		kind:         "response"
		method:       "get"
		op:           "getRenderedDashboard"
		path:         "/dashboards/{dashboard_id}/rendered"
		summary:      "Get rendered dashboard"
		cli: {
			command: ["dashboards", "get-rendered"]
		}
		returns:      "ResolvedDashboardDetail"
		error_family: "resource"
		params:       #renderedDashboardParameters
	},
	#plainDashboardOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateDashboard"
		path:         "/dashboards/{dashboard_id}"
		summary:      "Update dashboard"
		cli: {
			command: ["dashboards", "update"]
		}
		returns:      "Dashboard"
		error_family: "mutating"
		params:       #dashboardPathParameters
		body_ref:     "UpdateDashboardRequest"
		body_description: "Request payload"
	},
	#plainDashboardOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteDashboard"
		path:         "/dashboards/{dashboard_id}"
		summary:      "Delete dashboard"
		cli: {
			command: ["dashboards", "delete"]
		}
		error_family: "mutating"
		params:       #dashboardPathParameters
	},
	#plainDashboardOperation & {
		kind:         "response"
		method:       "get"
		op:           "listDashboardWidgets"
		path:         "/dashboards/{dashboard_id}/widgets"
		summary:      "List dashboard widgets"
		returns:      "DashboardWidgetList"
		error_family: "resource"
		params:       #dashboardPathParameters
	},
	#plainDashboardOperation & {
		kind:           "response"
		method:         "post"
		op:             "createDashboardWidget"
		path:           "/dashboards/{dashboard_id}/widgets"
		summary:        "Create dashboard widget"
		cli: {
			command: ["dashboards", "widgets", "create"]
		}
		returns:        "DashboardWidget"
		success_status: 201
		error_family:   "mutating"
		params:         #dashboardPathParameters
		body_ref:       "CreateDashboardWidgetRequest"
		body_description: "Request payload"
	},
	#plainDashboardOperation & {
		kind:         "response"
		method:       "get"
		op:           "getDashboardWidget"
		path:         "/dashboards/{dashboard_id}/widgets/{widget_id}"
		summary:      "Get dashboard widget"
		returns:      "DashboardWidget"
		error_family: "resource"
		params:       #dashboardWidgetPathParameters
	},
	#plainDashboardOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateDashboardWidget"
		path:         "/dashboards/{dashboard_id}/widgets/{widget_id}"
		summary:      "Update dashboard widget"
		cli: {
			command: ["dashboards", "widgets", "update"]
		}
		returns:      "DashboardWidget"
		error_family: "mutating"
		params:       #dashboardWidgetPathParameters
		body_ref:     "UpdateDashboardWidgetRequest"
		body_description: "Request payload"
	},
	#plainDashboardOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteDashboardWidget"
		path:         "/dashboards/{dashboard_id}/widgets/{widget_id}"
		summary:      "Delete dashboard widget"
		cli: {
			command: ["dashboards", "widgets", "delete"]
		}
		error_family: "mutating"
		params:       #dashboardWidgetPathParameters
	},
]

endpoints_dashboards: [
	for op in #dashboardOps {
		(#endpointFromGenericOperation & {
			tag:  #dashboardsTag
			spec: op
		}).endpoint
	},
]
