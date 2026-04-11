package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateModelRun"
		_table: "model_runs"
		params: [
			{name: "ID", type: "string"},
			{name: "Status", type: "string"},
			{name: "TriggerType", type: "string"},
			{name: "TriggeredBy", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "EnvironmentName", type: "string"},
			{name: "BuildID", type: "sql.NullString"},
			{name: "TargetCatalog", type: "string"},
			{name: "TargetSchema", type: "string"},
			{name: "ModelSelector", type: "string"},
			{name: "Variables", type: "string"},
			{name: "FullRefresh", type: "int64"},
			{name: "CompileManifest", type: "string"},
			{name: "CompileDiagnostics", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"status",
				"trigger_type",
				"triggered_by",
				"project_name",
				"environment_name",
				"build_id",
				"target_catalog",
				"target_schema",
				"model_selector",
				"variables",
				"full_refresh",
				"compile_manifest",
				"compile_diagnostics",
			]
			values: [
				{param: "ID"},
				{param: "Status"},
				{param: "TriggerType"},
				{param: "TriggeredBy"},
				{param: "ProjectName"},
				{param: "EnvironmentName"},
				{param: "BuildID"},
				{param: "TargetCatalog"},
				{param: "TargetSchema"},
				{param: "ModelSelector"},
				{param: "Variables"},
				{param: "FullRefresh"},
				{param: "CompileManifest"},
				{param: "CompileDiagnostics"},
			]
		}
	},
	#GetByID & {
		name:   "GetModelRunByID"
		_table: "model_runs"
	},
	{
		name: "UpdateModelRunBuild"
		kind: "exec"
		params: [
			{name: "BuildID", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "model_runs"
			set: [
				{column: "build_id", value: {param: "BuildID"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	{
		name: "UpdateModelRunFinished"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "model_runs"
			set: [
				{column: "status", value: {param: "Status"}},
				{column: "finished_at", value: {sql: "datetime('now')"}},
				{column: "error_message", value: {param: "ErrorMessage"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	{
		name: "UpdateModelRunStarted"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		update: {
			table: "model_runs"
			set: [
				{column: "status", value: {sql: "'RUNNING'"}},
				{column: "started_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
]
