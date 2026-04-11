package querydefs

queries: [
	{
		name:      "CountDashboards"
		kind:      "one"
		paramMode: "struct"
		params: [
			{name: "Owner", type: "sql.NullString"},
			{name: "FolderID", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from:    "dashboards"
			columns: [{expr: "COUNT(*)"}]
			where: [
				{column: "owner", op: "=", param: "Owner", optional: true},
				{column: "folder_id", op: "=", param: "FolderID", optional: true},
			]
		}
	},
	{
		name:      "ListDashboards"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "Owner", type: "sql.NullString"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
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
				{column: "owner", op: "=", param: "Owner", optional: true},
				{column: "folder_id", op: "=", param: "FolderID", optional: true},
			]
			orderBy:     [{expr: "updated_at", desc: true}]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name:      "ListDashboardsByFolders"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "FolderIDs", type: "[]sql.NullString"},
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
			where:   [{column: "folder_id", op: "=", param: "FolderIDs", slice: true}]
			orderBy: [{expr: "updated_at", desc: true}]
		}
	},
	{
		name:      "CountNotebooks"
		kind:      "one"
		paramMode: "struct"
		params: [
			{name: "Owner", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from:    "notebooks"
			columns: [{expr: "COUNT(*)"}]
			where:   [{column: "owner", op: "=", param: "Owner", optional: true}]
		}
	},
	{
		name:      "ListNotebooks"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "Owner", type: "sql.NullString"},
			{name: "Offset", type: "int64"},
			{name: "Limit", type: "int64"},
		]
		result: {
			row: "Notebook"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "GitRepoID", type: "sql.NullString"},
				{name: "GitPath", type: "sql.NullString"},
				{name: "FolderID", type: "sql.NullString"},
				{name: "ProjectOverrideID", type: "sql.NullString"},
				{name: "EnvironmentOverrideID", type: "sql.NullString"},
			]
		}
		select: {
			from: "notebooks"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "git_repo_id"},
				{expr: "git_path"},
				{expr: "folder_id"},
				{expr: "project_override_id"},
				{expr: "environment_override_id"},
			]
			where:       [{column: "owner", op: "=", param: "Owner", optional: true}]
			orderBy:     [{expr: "updated_at", desc: true}]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name:      "CountModels"
		kind:      "one"
		paramMode: "struct"
		params: [
			{name: "ProjectName", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from:    "models"
			columns: [{expr: "COUNT(*)"}]
			where:   [{column: "project_name", op: "=", param: "ProjectName", optional: true}]
		}
	},
	{
		name:      "ListModels"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "ProjectName", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Model"
			fields: [
				{name: "ID", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Name", type: "string"},
				{name: "SqlBody", type: "string"},
				{name: "Materialization", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Tags", type: "string"},
				{name: "DependsOn", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		select: {
			from: "models"
			columns: [
				{expr: "id"},
				{expr: "project_name"},
				{expr: "name"},
				{expr: "sql_body"},
				{expr: "materialization"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "tags"},
				{expr: "depends_on"},
				{expr: "config"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "contract"},
				{expr: "freshness_max_lag"},
				{expr: "freshness_cron"},
			]
			where: [
				{column: "project_name", op: "=", param: "ProjectName", optional: true},
			]
			orderBy: [
				{expr: "project_name"},
				{expr: "name"},
			]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name:      "CountModelRuns"
		kind:      "one"
		paramMode: "struct"
		params: [
			{name: "Status", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from:    "model_runs"
			columns: [{expr: "COUNT(*)"}]
			where:   [{column: "status", op: "=", param: "Status", optional: true}]
		}
	},
	{
		name:      "ListModelRuns"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "Status", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
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
			where:       [{column: "status", op: "=", param: "Status", optional: true}]
			orderBy:     [{expr: "created_at", desc: true}]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name:      "CountPipelineRuns"
		kind:      "one"
		paramMode: "struct"
		params: [
			{name: "PipelineID", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from:    "pipeline_runs"
			columns: [{expr: "COUNT(*)"}]
			where: [
				{column: "pipeline_id", op: "=", param: "PipelineID", optional: true},
				{column: "status", op: "=", param: "Status", optional: true},
			]
		}
	},
	{
		name:      "ListPipelineRuns"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "PipelineID", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "PipelineRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Status", type: "string"},
				{name: "TriggerType", type: "string"},
				{name: "TriggeredBy", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "GitCommitHash", type: "sql.NullString"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "pipeline_runs"
			columns: [
				{expr: "id"},
				{expr: "pipeline_id"},
				{expr: "status"},
				{expr: "trigger_type"},
				{expr: "triggered_by"},
				{expr: "parameters"},
				{expr: "git_commit_hash"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "created_at"},
			]
			where: [
				{column: "pipeline_id", op: "=", param: "PipelineID", optional: true},
				{column: "status", op: "=", param: "Status", optional: true},
			]
			orderBy:     [{expr: "created_at", desc: true}]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name:      "ListPipelinesByFolders"
		kind:      "many"
		paramMode: "struct"
		params: [
			{name: "FolderIDs", type: "[]sql.NullString"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		select: {
			from: "pipelines"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "schedule_cron"},
				{expr: "is_paused"},
				{expr: "concurrency_limit"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
			]
			where:   [{column: "folder_id", op: "=", param: "FolderIDs", slice: true}]
			orderBy: [{expr: "name"}]
		}
	},
]
