package querydefs

queries: [
	{
		name: "CountNotebookJobs"
		kind: "one"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "notebook_jobs"
			columns: [
				{expr: "COUNT(*)"},
			]
			where: [
				{column: "notebook_id", op: "=", param: "notebookID"},
			]
		}
	},
	{
		name: "CreateNotebookJob"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "SessionID", type: "string"},
			{name: "State", type: "string"},
		]
		result: {
			row: "NotebookJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "SessionID", type: "string"},
				{name: "State", type: "string"},
				{name: "Result", type: "sql.NullString"},
				{name: "Error", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "notebook_jobs"
			columns: ["id", "notebook_id", "session_id", "state"]
			values: [
				{param: "ID"},
				{param: "NotebookID"},
				{param: "SessionID"},
				{param: "State"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "session_id"},
				{expr: "state"},
				{expr: "result"},
				{expr: "error"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "GetNotebookJob"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "NotebookJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "SessionID", type: "string"},
				{name: "State", type: "string"},
				{name: "Result", type: "sql.NullString"},
				{name: "Error", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "notebook_jobs"
			columns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "session_id"},
				{expr: "state"},
				{expr: "result"},
				{expr: "error"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListNotebookJobs"
		kind: "many"
		params: [
			{name: "NotebookID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "NotebookJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "SessionID", type: "string"},
				{name: "State", type: "string"},
				{name: "Result", type: "sql.NullString"},
				{name: "Error", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "notebook_jobs"
			columns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "session_id"},
				{expr: "state"},
				{expr: "result"},
				{expr: "error"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "notebook_id", op: "=", param: "NotebookID"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "UpdateNotebookJobState"
		kind: "exec"
		params: [
			{name: "State", type: "string"},
			{name: "Result", type: "sql.NullString"},
			{name: "Error", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "notebook_jobs"
			set: [
				{column: "state", value: {param: "State"}},
				{column: "result", value: {param: "Result"}},
				{column: "error", value: {param: "Error"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
