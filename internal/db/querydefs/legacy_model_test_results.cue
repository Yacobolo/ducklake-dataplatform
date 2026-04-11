package querydefs

queries: [
	{
		name: "CreateModelTestResult"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "RunStepID", type: "string"},
			{name: "TestID", type: "string"},
			{name: "TestName", type: "string"},
			{name: "Status", type: "string"},
			{name: "RowsReturned", type: "sql.NullInt64"},
			{name: "ErrorMessage", type: "sql.NullString"},
		]
		result: {
			row: "ModelTestResult"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunStepID", type: "string"},
				{name: "TestID", type: "string"},
				{name: "TestName", type: "string"},
				{name: "Status", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		insert: {
			into: "model_test_results"
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
			returningColumns: [
				{expr: "id"},
				{expr: "run_step_id"},
				{expr: "test_id"},
				{expr: "test_name"},
				{expr: "status"},
				{expr: "rows_returned"},
				{expr: "error_message"},
				{expr: "created_at"},
			]
		}
	},
	{
		name: "ListModelTestResultsByStep"
		kind: "many"
		params: [
			{name: "runStepID", type: "string"},
		]
		result: {
			row: "ModelTestResult"
			fields: [
				{name: "ID", type: "string"},
				{name: "RunStepID", type: "string"},
				{name: "TestID", type: "string"},
				{name: "TestName", type: "string"},
				{name: "Status", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
				{name: "ErrorMessage", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "model_test_results"
			columns: [
				{expr: "id"},
				{expr: "run_step_id"},
				{expr: "test_id"},
				{expr: "test_name"},
				{expr: "status"},
				{expr: "rows_returned"},
				{expr: "error_message"},
				{expr: "created_at"},
			]
			where: [
				{column: "run_step_id", op: "=", param: "runStepID"},
			]
			orderBy: [
				{expr: "test_name"},
			]
		}
	},
]
