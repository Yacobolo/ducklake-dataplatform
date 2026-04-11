package querydefs

queries: [
	{
		name: "DeleteTableStatistics"
		kind: "exec"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		delete: {
			from: "table_statistics"
			where: [
				{column: "table_securable_name", op: "=", param: "tableSecurableName"},
			]
		}
	},
	{
		name: "DeleteTableStatisticsByPattern"
		kind: "exec"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		delete: {
			from: "table_statistics"
			where: [
				{column: "table_securable_name", op: "LIKE", param: "tableSecurableName"},
			]
		}
	},
	#GetByStringField & {
		name:   "GetTableStatistics"
		_table: "table_statistics"
		_field: "table_securable_name"
		_param: "tableSecurableName"
	},
	{
		name: "UpsertTableStatistics"
		kind: "exec"
		params: [
			{name: "TableSecurableName", type: "string"},
			{name: "RowCount", type: "sql.NullInt64"},
			{name: "SizeBytes", type: "sql.NullInt64"},
			{name: "ColumnCount", type: "sql.NullInt64"},
			{name: "ProfiledBy", type: "sql.NullString"},
		]
		insert: {
			into: "table_statistics"
			columns: [
				"table_securable_name",
				"row_count",
				"size_bytes",
				"column_count",
				"last_profiled_at",
				"profiled_by",
			]
			values: [
				{param: "TableSecurableName"},
				{param: "RowCount"},
				{param: "SizeBytes"},
				{param: "ColumnCount"},
				{sql: "datetime('now')"},
				{param: "ProfiledBy"},
			]
			conflict: {
				targets: [
					"table_securable_name",
				]
				doUpdate: [
					{column: "row_count", value: {sql: "excluded.row_count"}},
					{column: "size_bytes", value: {sql: "excluded.size_bytes"}},
					{column: "column_count", value: {sql: "excluded.column_count"}},
					{column: "last_profiled_at", value: {sql: "datetime('now')"}},
					{column: "profiled_by", value: {sql: "excluded.profiled_by"}},
				]
			}
		}
	},
]
