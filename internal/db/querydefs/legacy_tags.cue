package querydefs

queries: [
	{
		name: "CountTags"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "tags"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
		}
	},
	{
		name: "CreateTag"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Key", type: "string"},
			{name: "Value", type: "sql.NullString"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		insert: {
			into: "tags"
			columns: ["id", "key", "value", "created_by"]
			values: [
				{param: "ID"},
				{param: "Key"},
				{param: "Value"},
				{param: "CreatedBy"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "\"key\""},
				{expr: "value"},
				{expr: "created_by"},
				{expr: "created_at"},
			]
		}
	},
	{
		name: "DeleteTag"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "tags"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetTag"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "tags"
			columns: [
				{expr: "id"},
				{expr: "\"key\""},
				{expr: "value"},
				{expr: "created_by"},
				{expr: "created_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListTags"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "tags"
			columns: [
				{expr: "id"},
				{expr: "\"key\""},
				{expr: "value"},
				{expr: "created_by"},
				{expr: "created_at"},
			]
			orderBy: [
				{expr: "key"},
				{expr: "value"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "ListTagsForSecurable"
		kind: "many"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableID", type: "string"},
			{name: "ColumnName", type: "sql.NullString"},
		]
		result: {
			row: "Tag"
			fields: [
				{name: "ID", type: "string"},
				{name: "Key", type: "string"},
				{name: "Value", type: "sql.NullString"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
			]
		}
		select: {
			from: "tags"
			alias: "t"
			columns: [
				{expr: "t.id"},
				{expr: "t.\"key\""},
				{expr: "t.value"},
				{expr: "t.created_by"},
				{expr: "t.created_at"},
			]
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
