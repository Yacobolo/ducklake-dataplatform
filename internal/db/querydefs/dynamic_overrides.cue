package querydefs

queries: [
	#CountFiltered & {
		name:   "CountDashboards"
		_table: "dashboards"
		_params: [
			{name: "Owner", type: "sql.NullString"},
			{name: "FolderID", type: "sql.NullString"},
		]
		_where: [
			{column: "owner", op: "=", param: "Owner", optional: true},
			{column: "folder_id", op: "=", param: "FolderID", optional: true},
		]
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListDashboards"
		_table: "dashboards"
		_params: [
			{name: "Owner", type: "sql.NullString"},
			{name: "FolderID", type: "sql.NullString"},
		]
		_where: [
			{column: "owner", op: "=", param: "Owner", optional: true},
			{column: "folder_id", op: "=", param: "FolderID", optional: true},
		]
		_order: [
			{expr: "updated_at", desc: true},
		]
	},
	{
		name:      "ListDashboardsByFolders"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "FolderIDs", type: "[]sql.NullString"},
		]
		result: {table: "dashboards"}
		select: {
			from: "dashboards"
			where: [
				{column: "folder_id", op: "=", param: "FolderIDs", slice: true},
			]
			orderBy: [
				{expr: "updated_at", desc: true},
			]
		}
	},
	#CountFiltered & {
		name:   "CountNotebooks"
		_table: "notebooks"
		_params: [
			{name: "Owner", type: "sql.NullString"},
		]
		_where: [
			{column: "owner", op: "=", param: "Owner", optional: true},
		]
	},
	{
		name: "ListNotebooks"
		kind: "many"
		params: [
			{name: "Owner", type: "sql.NullString"},
			{name: "Offset", type: "int64"},
			{name: "Limit", type: "int64"},
		]
		result: {table: "notebooks"}
		select: {
			from: "notebooks"
			where: [
				{column: "owner", op: "=", param: "Owner", optional: true},
			]
			orderBy: [
				{expr: "updated_at", desc: true},
			]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	#CountFiltered & {
		name:   "CountModels"
		_table: "models"
		_params: [
			{name: "ProjectName", type: "sql.NullString"},
		]
		_where: [
			{column: "project_name", op: "=", param: "ProjectName", optional: true},
		]
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListModels"
		_table: "models"
		_params: [
			{name: "ProjectName", type: "sql.NullString"},
		]
		_where: [
			{column: "project_name", op: "=", param: "ProjectName", optional: true},
		]
		_order: [
			{expr: "project_name"},
			{expr: "name"},
		]
	},
	#CountFiltered & {
		name:   "CountModelRuns"
		_table: "model_runs"
		_params: [
			{name: "Status", type: "sql.NullString"},
		]
		_where: [
			{column: "status", op: "=", param: "Status", optional: true},
		]
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListModelRuns"
		_table: "model_runs"
		_params: [
			{name: "Status", type: "sql.NullString"},
		]
		_where: [
			{column: "status", op: "=", param: "Status", optional: true},
		]
		_order: [
			{expr: "created_at", desc: true},
		]
	},
	#CountFiltered & {
		name:   "CountPipelineRuns"
		_table: "pipeline_runs"
		_params: [
			{name: "PipelineID", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
		]
		_where: [
			{column: "pipeline_id", op: "=", param: "PipelineID", optional: true},
			{column: "status", op: "=", param: "Status", optional: true},
		]
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListPipelineRuns"
		_table: "pipeline_runs"
		_params: [
			{name: "PipelineID", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
		]
		_where: [
			{column: "pipeline_id", op: "=", param: "PipelineID", optional: true},
			{column: "status", op: "=", param: "Status", optional: true},
		]
		_order: [
			{expr: "created_at", desc: true},
		]
	},
	{
		name:      "ListPipelinesByFolders"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "FolderIDs", type: "[]sql.NullString"},
		]
		result: {table: "pipelines"}
		select: {
			from: "pipelines"
			where: [
				{column: "folder_id", op: "=", param: "FolderIDs", slice: true},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
]
