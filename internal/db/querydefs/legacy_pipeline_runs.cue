package querydefs

queries: [
	{
		name: "CancelPendingPipelineRuns"
		kind: "exec"
		params: [
			{name: "pipelineID", type: "string"},
		]
		update: {
			table: "pipeline_runs"
			set: [
				{column: "status", value: {sql: "'CANCELLED'"}},
			]
			where: [
				{column: "pipeline_id", op: "=", param: "pipelineID"},
				{column: "status", op: "=", valueSQL: "'PENDING'"},
			]
		}
	},
	{
		name: "CountActivePipelineRuns"
		kind: "one"
		params: [
			{name: "pipelineID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "pipeline_runs"
			columns: [
				{expr: "COUNT(*)"},
			]
			where: [
				{column: "pipeline_id", op: "=", param: "pipelineID"},
				{rawSQL: "status IN ('PENDING', 'RUNNING')"},
			]
		}
	},
	{
		name: "CreatePipelineRun"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "PipelineID", type: "string"},
			{name: "Status", type: "string"},
			{name: "TriggerType", type: "string"},
			{name: "TriggeredBy", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "GitCommitHash", type: "sql.NullString"},
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
		insert: {
			into: "pipeline_runs"
			columns: [
				"id",
				"pipeline_id",
				"status",
				"trigger_type",
				"triggered_by",
				"parameters",
				"git_commit_hash",
			]
			values: [
				{param: "ID"},
				{param: "PipelineID"},
				{param: "Status"},
				{param: "TriggerType"},
				{param: "TriggeredBy"},
				{param: "Parameters"},
				{param: "GitCommitHash"},
			]
			returningColumns: [
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
		}
	},
	{
		name: "GetPipelineRunByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
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
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "UpdatePipelineRunFinished"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "pipeline_runs"
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
		name: "UpdatePipelineRunStarted"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		update: {
			table: "pipeline_runs"
			set: [
				{column: "status", value: {sql: "'RUNNING'"}},
				{column: "started_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "UpdatePipelineRunStatus"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "pipeline_runs"
			set: [
				{column: "status", value: {param: "Status"}},
				{column: "error_message", value: {param: "ErrorMessage"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
