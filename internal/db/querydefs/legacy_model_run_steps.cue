package querydefs

queries: [
	{
		name: "CreateModelRunStep"
		kind: "one"
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
		result: {
			row: "ModelRunStep"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "ModelName", type: "string"},
				{name: "Status", type: "string"},
				{name: "Tier", type: "int64"},
				{name: "RowsAffected", type: "sql.NullInt64"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "CompiledSql", type: "sql.NullString"},
				{name: "CompiledHash", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "VarsUsed", type: "string"},
				{name: "MacrosUsed", type: "string"},
			]
		}
		insert: {
			into: "model_run_steps"
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
			returningColumns: [
				{expr: "id"},
				{expr: "run_id"},
				{expr: "model_id"},
				{expr: "model_name"},
				{expr: "status"},
				{expr: "tier"},
				{expr: "rows_affected"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "created_at"},
				{expr: "compiled_sql"},
				{expr: "compiled_hash"},
				{expr: "depends_on"},
				{expr: "vars_used"},
				{expr: "macros_used"},
			]
		}
	},
	{
		name: "ListModelRunStepsByRun"
		kind: "many"
		params: [
			{name: "runID", type: "string"},
		]
		result: {
			row: "ModelRunStep"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "ModelName", type: "string"},
				{name: "Status", type: "string"},
				{name: "Tier", type: "int64"},
				{name: "RowsAffected", type: "sql.NullInt64"},
				{name: "StartedAt", type: "sql.NullString"},
				{name: "FinishedAt", type: "sql.NullString"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "CompiledSql", type: "sql.NullString"},
				{name: "CompiledHash", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "VarsUsed", type: "string"},
				{name: "MacrosUsed", type: "string"},
			]
		}
		select: {
			from: "model_run_steps"
			columns: [
				{expr: "id"},
				{expr: "run_id"},
				{expr: "model_id"},
				{expr: "model_name"},
				{expr: "status"},
				{expr: "tier"},
				{expr: "rows_affected"},
				{expr: "started_at"},
				{expr: "finished_at"},
				{expr: "error_message"},
				{expr: "created_at"},
				{expr: "compiled_sql"},
				{expr: "compiled_hash"},
				{expr: "depends_on"},
				{expr: "vars_used"},
				{expr: "macros_used"},
			]
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
