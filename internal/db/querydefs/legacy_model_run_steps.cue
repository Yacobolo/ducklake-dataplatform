package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateModelRunStep"
		_table: "model_run_steps"
		params: [
			{name: "ID", type: "string"},
			{name: "RunID", type: "string"},
			{name: "ModelID", type: "string"},
			{name: "ModelName", type: "string"},
			{name: "CompiledSql", type: "sql.NullString"},
			{name: "CompiledHash", type: "sql.NullString"},
			{name: "DependsOn", type: "string"},
			{name: "VarsUsed", type: "string"},
			{name: "MacrosUsed", type: "string"},
			{name: "Status", type: "string"},
			{name: "Tier", type: "int64"},
		]
		insert: {
			columns: [
				"id",
				"run_id",
				"model_id",
				"model_name",
				"compiled_sql",
				"compiled_hash",
				"depends_on",
				"vars_used",
				"macros_used",
				"status",
				"tier",
			]
			values: [
				{param: "ID"},
				{param: "RunID"},
				{param: "ModelID"},
				{param: "ModelName"},
				{param: "CompiledSql"},
				{param: "CompiledHash"},
				{param: "DependsOn"},
				{param: "VarsUsed"},
				{param: "MacrosUsed"},
				{param: "Status"},
				{param: "Tier"},
			]
		}
	},
	{
		name: "ListModelRunStepsByRun"
		kind: "many"
		params: [
			{name: "runID", type: "string"},
		]
		result: {table: "model_run_steps"}
		select: {
			from: "model_run_steps"
			where: [
				{column: "run_id", op: "=", param: "runID"},
			]
			orderBy: [
				{expr: "tier"},
				{expr: "model_name"},
			]
		}
	},
	{
		name: "UpdateModelRunStepFinished"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "RowsAffected", type: "sql.NullInt64"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "model_run_steps"
			set: [
				{column: "status", value: {param: "Status"}},
				{column: "finished_at", value: {sql: "datetime('now')"}},
				{column: "rows_affected", value: {param: "RowsAffected"}},
				{column: "error_message", value: {param: "ErrorMessage"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	{
		name: "UpdateModelRunStepStarted"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		update: {
			table: "model_run_steps"
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
