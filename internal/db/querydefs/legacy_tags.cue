package querydefs

queries: [
	#CountAll & {
		name:   "CountTags"
		_table: "tags"
	},
	#InsertReturningTable & {
		name:   "CreateTag"
		_table: "tags"
		params: [
			{name: "ID", type: "string"},
			{name: "Key", type: "string"},
			{name: "Value", type: "sql.NullString"},
			{name: "CreatedBy", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"key",
				"value",
				"created_by",
			]
			values: [
				{param: "ID"},
				{param: "Key"},
				{param: "Value"},
				{param: "CreatedBy"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteTag"
		_table: "tags"
	},
	#GetByID & {
		name:   "GetTag"
		_table: "tags"
	},
	#ListPaginatedOrdered & {
		name:   "ListTags"
		_table: "tags"
		_order: [
			{expr: "key"},
			{expr: "value"},
		]
	},
	{
		name: "ListTagsForSecurable"
		kind: "many"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "ColumnName", type: "sql.NullString"},
		]
		result: {table: "tags"}
		select: {
			from:  "tags"
			alias: "t"
			joins: [
				{type: "JOIN", table: "tag_assignments", alias: "ta", on: "t.id = ta.tag_id"},
			]
			where: [
				{column: "ta.securable_type", op: "=", param: "SecurableType"},
				{column: "ta.securable_id", op: "=", param: "SecurableID"},
				{column: "ta.column_name", op: "=", param: "ColumnName", optional: true},
			]
			orderBy: [
				{expr: "t.key"},
				{expr: "t.value"},
			]
		}
	},
]
