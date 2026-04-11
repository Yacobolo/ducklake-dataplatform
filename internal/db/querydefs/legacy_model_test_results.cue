package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateModelTestResult"
		_table: "model_test_results"
		params: [
			{name: "ID", type: "string"},
			{name: "RunStepID", type: "string"},
			{name: "TestID", type: "string"},
			{name: "TestName", type: "string"},
			{name: "Status", type: "string"},
			{name: "RowsReturned", type: "sql.NullInt64"},
			{name: "ErrorMessage", type: "sql.NullString"},
		]
		insert: {
			columns: [
				"id",
				"run_step_id",
				"test_id",
				"test_name",
				"status",
				"rows_returned",
				"error_message",
			]
			values: [
				{param: "ID"},
				{param: "RunStepID"},
				{param: "TestID"},
				{param: "TestName"},
				{param: "Status"},
				{param: "RowsReturned"},
				{param: "ErrorMessage"},
			]
		}
	},
	{
		name: "ListModelTestResultsByStep"
		kind: "many"
		params: [
			{name: "runStepID", type: "string"},
		]
		result: {table: "model_test_results"}
		select: {
			from: "model_test_results"
			where: [
				{column: "run_step_id", op: "=", param: "runStepID"},
			]
			orderBy: [
				{expr: "test_name"},
			]
		}
	},
]
