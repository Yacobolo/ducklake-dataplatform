package querydefs

queries: [
	{
		name: "DeleteColumnMetadataByTable"
		kind: "exec"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		delete: {
			from: "column_metadata"
			where: [
				{column: "table_securable_name", op: "=", param: "tableSecurableName"},
			]
		}
	},
	{
		name: "DeleteColumnMetadataByTablePattern"
		kind: "exec"
		params: [
			{name: "tableSecurableName", type: "string"},
		]
		delete: {
			from: "column_metadata"
			where: [
				{column: "table_securable_name", op: "LIKE", param: "tableSecurableName"},
			]
		}
	},
	{
		name: "GetColumnMetadata"
		kind: "one"
		params: [
			{name: "TableSecurableName", type: "string"},
			{name: "ColumnName", type: "string"},
		]
		result: {
			row: "ColumnMetadatum"
			fields: [
				{name: "TableSecurableName", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "UpdatedAt", type: "sql.NullString"},
			]
		}
		select: {
			from: "column_metadata"
			columns: [
				{expr: "table_securable_name"},
				{expr: "column_name"},
				{expr: "comment"},
				{expr: "properties"},
				{expr: "updated_at"},
			]
			where: [
				{column: "table_securable_name", op: "=", param: "TableSecurableName"},
				{column: "column_name", op: "=", param: "ColumnName"},
			]
		}
	},
	{
		name: "UpsertColumnMetadata"
		kind: "exec"
		params: [
			{name: "TableSecurableName", type: "string"},
			{name: "ColumnName", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
		]
		insert: {
			into: "column_metadata"
			columns: [
				"table_securable_name",
				"column_name",
				"comment",
				"properties",
			]
			values: [
				{param: "TableSecurableName"},
				{param: "ColumnName"},
				{param: "Comment"},
				{param: "Properties"},
			]
			conflict: {
				targets: [
					"table_securable_name",
					"column_name",
				]
				doUpdate: [
					{column: "comment", value: {sql: "COALESCE(excluded.comment, comment)"}},
					{column: "properties", value: {sql: "COALESCE(excluded.properties, properties)"}},
					{column: "updated_at", value: {sql: "datetime('now')"}},
				]
			}
		}
	},
]
