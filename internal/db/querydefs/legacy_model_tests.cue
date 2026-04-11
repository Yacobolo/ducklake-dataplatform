package querydefs

queries: [
	{
		name: "CreateModelTest"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "ModelID", type: "string"},
			{name: "Name", type: "string"},
			{name: "TestType", type: "string"},
			{name: "ColumnName", type: "string"},
			{name: "Config", type: "string"},
		]
		result: {
			row: "ModelTest"
			fields: [
				{name: "ID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "TestType", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		insert: {
			into: "model_tests"
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
			returningColumns: [
				{expr: "id"},
				{expr: "model_id"},
				{expr: "name"},
				{expr: "test_type"},
				{expr: "column_name"},
				{expr: "config"},
				{expr: "created_at"},
			]
		}
	},
	{
		name: "DeleteModelTest"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "model_tests"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetModelTestByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ModelTest"
			fields: [
				{name: "ID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "TestType", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "model_tests"
			columns: [
				{expr: "id"},
				{expr: "model_id"},
				{expr: "name"},
				{expr: "test_type"},
				{expr: "column_name"},
				{expr: "config"},
				{expr: "created_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListModelTestsByModel"
		kind: "many"
		params: [
			{name: "modelID", type: "string"},
		]
		result: {
			row: "ModelTest"
			fields: [
				{name: "ID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "TestType", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Config", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "model_tests"
			columns: [
				{expr: "id"},
				{expr: "model_id"},
				{expr: "name"},
				{expr: "test_type"},
				{expr: "column_name"},
				{expr: "config"},
				{expr: "created_at"},
			]
			where: [
				{column: "model_id", op: "=", param: "modelID"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
]
