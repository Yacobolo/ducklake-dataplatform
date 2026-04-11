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
	#CountFiltered & {
		name:   "CountActivePipelineRuns"
		_table: "pipeline_runs"
		_params: [
			{name: "pipelineID", type: "string"},
		]
		_where: [
			{column: "pipeline_id", op: "=", param: "pipelineID"},
			{rawSQL: "status IN ('PENDING', 'RUNNING')"},
		]
	},
	#InsertReturningTable & {
		name:   "CreatePipelineRun"
		_table: "pipeline_runs"
		params: [
			{name: "ID", type: "string"},
			{name: "PipelineID", type: "string"},
			{name: "Status", type: "string"},
			{name: "TriggerType", type: "string"},
			{name: "TriggeredBy", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "GitCommitHash", type: "sql.NullString"},
		]
		insert: {
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
		}
	},
	#GetByID & {
		name:   "GetPipelineRunByID"
		_table: "pipeline_runs"
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
