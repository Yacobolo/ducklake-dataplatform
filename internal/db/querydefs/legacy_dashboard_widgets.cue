package querydefs

queries: [
	{
		name: "CreateDashboardWidget"
		kind: "one"
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
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		insert: {
			into: "dashboard_widgets"
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
			returningColumns: [
				{expr: "id"},
				{expr: "dashboard_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "source_json"},
				{expr: "visual_spec"},
				{expr: "layout_x"},
				{expr: "layout_y"},
				{expr: "layout_w"},
				{expr: "layout_h"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "filter_origin_key"},
				{expr: "page_name"},
			]
		}
	},
	{
		name: "DeleteDashboardWidget"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "dashboard_widgets"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetDashboardWidget"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		select: {
			from: "dashboard_widgets"
			columns: [
				{expr: "id"},
				{expr: "dashboard_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "source_json"},
				{expr: "visual_spec"},
				{expr: "layout_x"},
				{expr: "layout_y"},
				{expr: "layout_w"},
				{expr: "layout_h"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "filter_origin_key"},
				{expr: "page_name"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListDashboardWidgetsByDashboard"
		kind: "many"
		params: [
			{name: "dashboardID", type: "string"},
		]
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		select: {
			from: "dashboard_widgets"
			columns: [
				{expr: "id"},
				{expr: "dashboard_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "source_json"},
				{expr: "visual_spec"},
				{expr: "layout_x"},
				{expr: "layout_y"},
				{expr: "layout_w"},
				{expr: "layout_h"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "filter_origin_key"},
				{expr: "page_name"},
			]
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
	{
		name: "UpdateDashboardWidget"
		kind: "one"
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
		result: {
			row: "DashboardWidget"
			fields: [
				{name: "ID", type: "string"},
				{name: "DashboardID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "SourceJson", type: "string"},
				{name: "VisualSpec", type: "string"},
				{name: "LayoutX", type: "int64"},
				{name: "LayoutY", type: "int64"},
				{name: "LayoutW", type: "int64"},
				{name: "LayoutH", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FilterOriginKey", type: "string"},
				{name: "PageName", type: "string"},
			]
		}
		update: {
			table: "dashboard_widgets"
			set: [
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
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "dashboard_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "source_json"},
				{expr: "visual_spec"},
				{expr: "layout_x"},
				{expr: "layout_y"},
				{expr: "layout_w"},
				{expr: "layout_h"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "filter_origin_key"},
				{expr: "page_name"},
			]
		}
	},
]
