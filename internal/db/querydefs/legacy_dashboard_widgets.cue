package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateDashboardWidget"
		_table: "dashboard_widgets"
		params: [
			{name: "ID", type: "string"},
			{name: "DashboardID", type: "string"},
			{name: "FilterOriginKey", type: "string"},
			{name: "PageName", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "SourceJson", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "LayoutX", type: "int64"},
			{name: "LayoutY", type: "int64"},
			{name: "LayoutW", type: "int64"},
			{name: "LayoutH", type: "int64"},
		]
		insert: {
			columns: [
				"id",
				"dashboard_id",
				"filter_origin_key",
				"page_name",
				"name",
				"description",
				"source_json",
				"visual_spec",
				"layout_x",
				"layout_y",
				"layout_w",
				"layout_h",
			]
			values: [
				{param: "ID"},
				{param: "DashboardID"},
				{param: "FilterOriginKey"},
				{param: "PageName"},
				{param: "Name"},
				{param: "Description"},
				{param: "SourceJson"},
				{param: "VisualSpec"},
				{param: "LayoutX"},
				{param: "LayoutY"},
				{param: "LayoutW"},
				{param: "LayoutH"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteDashboardWidget"
		_table: "dashboard_widgets"
	},
	#GetByID & {
		name:   "GetDashboardWidget"
		_table: "dashboard_widgets"
	},
	{
		name: "ListDashboardWidgetsByDashboard"
		kind: "many"
		params: [
			{name: "dashboardID", type: "string"},
		]
		result: {table: "dashboard_widgets"}
		select: {
			from: "dashboard_widgets"
			where: [
				{column: "dashboard_id", op: "=", param: "dashboardID"},
			]
			orderBy: [
				{expr: "layout_y"},
				{expr: "layout_x"},
				{expr: "created_at"},
			]
		}
	},
	#UpdateByIDTouch & {
		name:   "UpdateDashboardWidget"
		_table: "dashboard_widgets"
		_kind:  "one"
		params: [
			{name: "FilterOriginKey", type: "string"},
			{name: "PageName", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "SourceJson", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "LayoutX", type: "int64"},
			{name: "LayoutY", type: "int64"},
			{name: "LayoutW", type: "int64"},
			{name: "LayoutH", type: "int64"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "filter_origin_key", value: {param: "FilterOriginKey"}},
			{column: "page_name", value: {param: "PageName"}},
			{column: "name", value: {param: "Name"}},
			{column: "description", value: {param: "Description"}},
			{column: "source_json", value: {param: "SourceJson"}},
			{column: "visual_spec", value: {param: "VisualSpec"}},
			{column: "layout_x", value: {param: "LayoutX"}},
			{column: "layout_y", value: {param: "LayoutY"}},
			{column: "layout_w", value: {param: "LayoutW"}},
			{column: "layout_h", value: {param: "LayoutH"}},
		]
	},
]
