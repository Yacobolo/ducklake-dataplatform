package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreatePipelineJobRun"
		_table: "pipeline_job_runs"
		params: [
			{name: "ID", type: "string"},
			{name: "RunID", type: "string"},
			{name: "JobID", type: "string"},
			{name: "JobName", type: "string"},
			{name: "Status", type: "string"},
			{name: "RetryAttempt", type: "int64"},
		]
		insert: {
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
		}
	},
	#GetByID & {
		name:   "GetPipelineJobRunByID"
		_table: "pipeline_job_runs"
	},
	{
		name: "ListPipelineJobRunsByRun"
		kind: "many"
		params: [
			{name: "runID", type: "string"},
		]
		result: {table: "pipeline_job_runs"}
		select: {
			from: "pipeline_job_runs"
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
