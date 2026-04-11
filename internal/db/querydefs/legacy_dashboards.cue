package querydefs

queries: [
	{
		name: "CreateDashboard"
		kind: "one"
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
		result: {
			row: "Dashboard"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "SemanticProjectName", type: "string"},
				{name: "SemanticModelName", type: "string"},
				{name: "ComputeMode", type: "string"},
				{name: "ComputeEndpointName", type: "string"},
				{name: "ComputeFallbackLocal", type: "int64"},
			]
		}
		insert: {
			into: "dashboards"
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
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
				{expr: "semantic_project_name"},
				{expr: "semantic_model_name"},
				{expr: "compute_mode"},
				{expr: "compute_endpoint_name"},
				{expr: "compute_fallback_local"},
			]
		}
	},
	{
		name: "DeleteDashboard"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "dashboards"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetDashboard"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Dashboard"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "SemanticProjectName", type: "string"},
				{name: "SemanticModelName", type: "string"},
				{name: "ComputeMode", type: "string"},
				{name: "ComputeEndpointName", type: "string"},
				{name: "ComputeFallbackLocal", type: "int64"},
			]
		}
		select: {
			from: "dashboards"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
				{expr: "semantic_project_name"},
				{expr: "semantic_model_name"},
				{expr: "compute_mode"},
				{expr: "compute_endpoint_name"},
				{expr: "compute_fallback_local"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "UpdateDashboard"
		kind: "one"
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
		result: {
			row: "Dashboard"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "SemanticProjectName", type: "string"},
				{name: "SemanticModelName", type: "string"},
				{name: "ComputeMode", type: "string"},
				{name: "ComputeEndpointName", type: "string"},
				{name: "ComputeFallbackLocal", type: "int64"},
			]
		}
		update: {
			table: "dashboards"
			set: [
				{column: "name", value: {param: "Name"}},
				{column: "description", value: {param: "Description"}},
				{column: "owner", value: {param: "Owner"}},
				{column: "folder_id", value: {param: "FolderID"}},
				{column: "semantic_project_name", value: {param: "SemanticProjectName"}},
				{column: "semantic_model_name", value: {param: "SemanticModelName"}},
				{column: "compute_mode", value: {param: "ComputeMode"}},
				{column: "compute_endpoint_name", value: {param: "ComputeEndpointName"}},
				{column: "compute_fallback_local", value: {param: "ComputeFallbackLocal"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
				{expr: "semantic_project_name"},
				{expr: "semantic_model_name"},
				{expr: "compute_mode"},
				{expr: "compute_endpoint_name"},
				{expr: "compute_fallback_local"},
			]
		}
	},
]
