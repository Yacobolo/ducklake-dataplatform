package querydefs

queries: [
	{
		name: "CreateModelRun"
		kind: "one"
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
		result: {
			row: "ModelRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "Status", type: "string"},
				{name: "TriggerType", type: "string"},
				{name: "TriggeredBy", type: "string"},
				{name: "TargetCatalog", type: "string"},
				{name: "TargetSchema", type: "string"},
				{name: "ModelSelector", type: "string"},
				{name: "Variables", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "FullRefresh", type: "int64"},
				{name: "CompileManifest", type: "string"},
				{name: "CompileDiagnostics", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "EnvironmentName", type: "string"},
				{name: "BuildID", type: "sql.NullString"},
			]
		}
		insert: {
			into: "model_runs"
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
			returningColumns: [
				{expr: "id"},
				{expr: "status"},
				{expr: "trigger_type"},
				{expr: "triggered_by"},
				{expr: "target_catalog"},
				{expr: "target_schema"},
				{expr: "model_selector"},
				{expr: "variables"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "created_at"},
				{expr: "full_refresh"},
				{expr: "compile_manifest"},
				{expr: "compile_diagnostics"},
				{expr: "project_name"},
				{expr: "environment_name"},
				{expr: "build_id"},
			]
		}
	},
	{
		name: "GetModelRunByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ModelRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "Status", type: "string"},
				{name: "TriggerType", type: "string"},
				{name: "TriggeredBy", type: "string"},
				{name: "TargetCatalog", type: "string"},
				{name: "TargetSchema", type: "string"},
				{name: "ModelSelector", type: "string"},
				{name: "Variables", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "FullRefresh", type: "int64"},
				{name: "CompileManifest", type: "string"},
				{name: "CompileDiagnostics", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "EnvironmentName", type: "string"},
				{name: "BuildID", type: "sql.NullString"},
			]
		}
		select: {
			from: "model_runs"
			columns: [
				{expr: "id"},
				{expr: "status"},
				{expr: "trigger_type"},
				{expr: "triggered_by"},
				{expr: "target_catalog"},
				{expr: "target_schema"},
				{expr: "model_selector"},
				{expr: "variables"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "created_at"},
				{expr: "full_refresh"},
				{expr: "compile_manifest"},
				{expr: "compile_diagnostics"},
				{expr: "project_name"},
				{expr: "environment_name"},
				{expr: "build_id"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
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
