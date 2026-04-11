package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateModelTest"
		_table: "model_tests"
		params: [
			{name: "ID", type: "string"},
			{name: "ModelID", type: "string"},
			{name: "Name", type: "string"},
			{name: "TestType", type: "string"},
			{name: "ColumnName", type: "string"},
			{name: "Config", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"model_id",
				"name",
				"test_type",
				"column_name",
				"config",
			]
			values: [
				{param: "ID"},
				{param: "ModelID"},
				{param: "Name"},
				{param: "TestType"},
				{param: "ColumnName"},
				{param: "Config"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteModelTest"
		_table: "model_tests"
	},
	#GetByID & {
		name:   "GetModelTestByID"
		_table: "model_tests"
	},
	{
		name: "ListModelTestsByModel"
		kind: "many"
		params: [
			{name: "modelID", type: "string"},
		]
		result: {table: "model_tests"}
		select: {
			from: "model_tests"
			where: [
				{column: "model_id", op: "=", param: "modelID"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
]
