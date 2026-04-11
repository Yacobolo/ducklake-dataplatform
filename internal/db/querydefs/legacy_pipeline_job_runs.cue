package querydefs

queries: [
	{
		name: "CreatePipelineJobRun"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "RunID", type: "string"},
			{name: "JobID", type: "string"},
			{name: "JobName", type: "string"},
			{name: "Status", type: "string"},
			{name: "RetryAttempt", type: "int64"},
		]
		result: {
			row: "PipelineJobRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "JobID", type: "string"},
				{name: "JobName", type: "string"},
				{name: "Status", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "RetryAttempt", type: "int64"},
				{name: "CreatedAt", type: "string"},
			]
		}
		insert: {
			into: "pipeline_job_runs"
			columns: [
				"id",
				"run_id",
				"job_id",
				"job_name",
				"status",
				"retry_attempt",
			]
			values: [
				{param: "ID"},
				{param: "RunID"},
				{param: "JobID"},
				{param: "JobName"},
				{param: "Status"},
				{param: "RetryAttempt"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "run_id"},
				{expr: "job_id"},
				{expr: "job_name"},
				{expr: "status"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "retry_attempt"},
				{expr: "created_at"},
			]
		}
	},
	{
		name: "GetPipelineJobRunByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "PipelineJobRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "JobID", type: "string"},
				{name: "JobName", type: "string"},
				{name: "Status", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "RetryAttempt", type: "int64"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "pipeline_job_runs"
			columns: [
				{expr: "id"},
				{expr: "run_id"},
				{expr: "job_id"},
				{expr: "job_name"},
				{expr: "status"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "retry_attempt"},
				{expr: "created_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListPipelineJobRunsByRun"
		kind: "many"
		params: [
			{name: "runID", type: "string"},
		]
		result: {
			row: "PipelineJobRun"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "JobID", type: "string"},
				{name: "JobName", type: "string"},
				{name: "Status", type: "string"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "RetryAttempt", type: "int64"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "pipeline_job_runs"
			columns: [
				{expr: "id"},
				{expr: "run_id"},
				{expr: "job_id"},
				{expr: "job_name"},
				{expr: "status"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "retry_attempt"},
				{expr: "created_at"},
			]
			where: [
				{column: "run_id", op: "=", param: "runID"},
			]
			orderBy: [
				{expr: "created_at"},
			]
		}
	},
	{
		name: "UpdatePipelineJobRunFinished"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "pipeline_job_runs"
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
		name: "UpdatePipelineJobRunStarted"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		update: {
			table: "pipeline_job_runs"
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
		name: "UpdatePipelineJobRunStatus"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "pipeline_job_runs"
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
