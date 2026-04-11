package querydefs

queries: [
	#CountFiltered & {
		name:   "CountViews"
		_table: "views"
		_params: [
			{name: "schemaID", type: "string"},
		]
		_where: [
			{column: "schema_id", op: "=", param: "schemaID"},
			{column: "deleted_at", op: "IS", valueSQL: "NULL"},
		]
	},
	#InsertReturningTable & {
		name:   "CreateView"
		_table: "views"
		params: [
			{name: "ID", type: "string"},
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
			{name: "ViewDefinition", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
			{name: "Owner", type: "string"},
			{name: "SourceTables", type: "sql.NullString"},
		]
		insert: {
			columns: [
				"id",
				"schema_id",
				"name",
				"view_definition",
				"comment",
				"properties",
				"owner",
				"source_tables",
			]
			values: [
				{param: "ID"},
				{param: "SchemaID"},
				{param: "Name"},
				{param: "ViewDefinition"},
				{param: "Comment"},
				{param: "Properties"},
				{param: "Owner"},
				{param: "SourceTables"},
			]
		}
	},
	{
		name: "DeleteView"
		kind: "exec"
		params: [
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
		]
		update: {
			table: "views"
			set: [
				{column: "deleted_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "schema_id", op: "=", param: "SchemaID"},
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
	{
		name: "DeleteViewsBySchema"
		kind: "exec"
		params: [
			{name: "schemaID", type: "string"},
		]
		update: {
			table: "views"
			set: [
				{column: "deleted_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "schema_id", op: "=", param: "schemaID"},
			]
		}
	},
	{
		name: "GetViewByName"
		kind: "one"
		params: [
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {table: "views"}
		select: {
			from: "views"
			where: [
				{column: "schema_id", op: "=", param: "SchemaID"},
				{column: "name", op: "=", param: "Name"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListViews"
		_table: "views"
		_params: [
			{name: "SchemaID", type: "string"},
		]
		_where: [
			{column: "schema_id", op: "=", param: "SchemaID"},
			{column: "deleted_at", op: "IS", valueSQL: "NULL"},
		]
		_order: [
			{expr: "name"},
		]
	},
	{
		name: "UpdateView"
		kind: "exec"
		params: [
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
			{name: "ViewDefinition", type: "string"},
			{name: "SourceTables", type: "sql.NullString"},
			{name: "SchemaID", type: "string"},
			{name: "Name", type: "string"},
		]
		update: {
			table: "views"
			set: [
				{column: "comment", value: {param: "Comment"}},
				{column: "properties", value: {param: "Properties"}},
				{column: "view_definition", value: {param: "ViewDefinition"}},
				{column: "source_tables", value: {param: "SourceTables"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "schema_id", op: "=", param: "SchemaID"},
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
]
