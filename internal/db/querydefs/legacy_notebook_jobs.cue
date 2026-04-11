package querydefs

queries: [
	#CountFiltered & {
		name:   "CountNotebookJobs"
		_table: "notebook_jobs"
		_params: [
			{name: "notebookID", type: "string"},
		]
		_where: [
			{column: "notebook_id", op: "=", param: "notebookID"},
		]
	},
	#InsertReturningTable & {
		name:   "CreateNotebookJob"
		_table: "notebook_jobs"
		params: [
			{name: "ID", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "SessionID", type: "string"},
			{name: "State", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"notebook_id",
				"session_id",
				"state",
			]
			values: [
				{param: "ID"},
				{param: "NotebookID"},
				{param: "SessionID"},
				{param: "State"},
			]
		}
	},
	#GetByID & {
		name:   "GetNotebookJob"
		_table: "notebook_jobs"
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListNotebookJobs"
		_table: "notebook_jobs"
		_params: [
			{name: "NotebookID", type: "string"},
		]
		_where: [
			{column: "notebook_id", op: "=", param: "NotebookID"},
		]
		_order: [
			{expr: "created_at", desc: true},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateNotebookJobState"
		_table: "notebook_jobs"
		params: [
			{name: "State", type: "string"},
			{name: "Result", type: "sql.NullString"},
			{name: "Error", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "state", value: {param: "State"}},
			{column: "result", value: {param: "Result"}},
			{column: "error", value: {param: "Error"}},
		]
	},
]
