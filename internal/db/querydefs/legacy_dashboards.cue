package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateDashboard"
		_table: "dashboards"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "SemanticProjectName", type: "string"},
			{name: "SemanticModelName", type: "string"},
			{name: "ComputeMode", type: "string"},
			{name: "ComputeEndpointName", type: "string"},
			{name: "ComputeFallbackLocal", type: "int64"},
		]
		insert: {
			columns: [
				"id",
				"name",
				"description",
				"owner",
				"folder_id",
				"semantic_project_name",
				"semantic_model_name",
				"compute_mode",
				"compute_endpoint_name",
				"compute_fallback_local",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Description"},
				{param: "Owner"},
				{param: "FolderID"},
				{param: "SemanticProjectName"},
				{param: "SemanticModelName"},
				{param: "ComputeMode"},
				{param: "ComputeEndpointName"},
				{param: "ComputeFallbackLocal"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteDashboard"
		_table: "dashboards"
	},
	#GetByID & {
		name:   "GetDashboard"
		_table: "dashboards"
	},
	#UpdateByIDTouch & {
		name:   "UpdateDashboard"
		_table: "dashboards"
		_kind:  "one"
		params: [
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "SemanticProjectName", type: "string"},
			{name: "SemanticModelName", type: "string"},
			{name: "ComputeMode", type: "string"},
			{name: "ComputeEndpointName", type: "string"},
			{name: "ComputeFallbackLocal", type: "int64"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "name", value: {param: "Name"}},
			{column: "description", value: {param: "Description"}},
			{column: "owner", value: {param: "Owner"}},
			{column: "folder_id", value: {param: "FolderID"}},
			{column: "semantic_project_name", value: {param: "SemanticProjectName"}},
			{column: "semantic_model_name", value: {param: "SemanticModelName"}},
			{column: "compute_mode", value: {param: "ComputeMode"}},
			{column: "compute_endpoint_name", value: {param: "ComputeEndpointName"}},
			{column: "compute_fallback_local", value: {param: "ComputeFallbackLocal"}},
		]
	},
]
