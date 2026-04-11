package querydefs

queries: [
	{
		name: "DeleteExternalTableColumns"
		kind: "exec"
		params: [
			{name: "externalTableID", type: "string"},
		]
		delete: {
			from: "external_table_columns"
			where: [
				{column: "external_table_id", op: "=", param: "externalTableID"},
			]
		}
	},
	{
		name: "InsertExternalTableColumn"
		kind: "exec"
		params: [
			{name: "ID", type: "string"},
			{name: "ExternalTableID", type: "string"},
			{name: "ColumnName", type: "string"},
			{name: "ColumnType", type: "string"},
			{name: "Position", type: "int64"},
		]
		insert: {
			into: "external_table_columns"
			columns: [
				"id",
				"external_table_id",
				"column_name",
				"column_type",
				"position",
			]
			values: [
				{param: "ID"},
				{param: "ExternalTableID"},
				{param: "ColumnName"},
				{param: "ColumnType"},
				{param: "Position"},
			]
		}
	},
	{
		name: "ListExternalTableColumns"
		kind: "many"
		params: [
			{name: "externalTableID", type: "string"},
		]
		result: {
			row: "ExternalTableColumn"
			fields: [
				{name: "ID", type: "string"},
				{name: "ExternalTableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "ColumnType", type: "string"},
				{name: "Position", type: "int64"},
			]
		}
		select: {
			from: "external_table_columns"
			columns: [
				{expr: "id"},
				{expr: "external_table_id"},
				{expr: "column_name"},
				{expr: "column_type"},
				{expr: "position"},
			]
			where: [
				{column: "external_table_id", op: "=", param: "externalTableID"},
			]
			orderBy: [
				{expr: "position"},
			]
		}
	},
]
