package querydefs

queries: [
	#CountFiltered & {
		name:   "CountExternalTables"
		_table: "external_tables"
		_params: [
			{name: "schemaName", type: "string"},
		]
		_where: [
			{column: "schema_name", op: "=", param: "schemaName"},
			{column: "deleted_at", op: "IS", valueSQL: "NULL"},
		]
	},
	#InsertReturningTable & {
		name:   "CreateExternalTable"
		_table: "external_tables"
		params: [
			{name: "ID", type: "string"},
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
			{name: "FileFormat", type: "string"},
			{name: "SourcePath", type: "string"},
			{name: "LocationName", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "CatalogName", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"schema_name",
				"table_name",
				"file_format",
				"source_path",
				"location_name",
				"comment",
				"owner",
				"catalog_name",
			]
			values: [
				{param: "ID"},
				{param: "SchemaName"},
				{param: "TableName"},
				{param: "FileFormat"},
				{param: "SourcePath"},
				{param: "LocationName"},
				{param: "Comment"},
				{param: "Owner"},
				{param: "CatalogName"},
			]
		}
	},
	{
		name: "GetExternalTableByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {table: "external_tables"}
		select: {
			from: "external_tables"
			where: [
				{column: "id", op: "=", param: "id"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	{
		name: "GetExternalTableByName"
		kind: "one"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
		]
		result: {table: "external_tables"}
		select: {
			from: "external_tables"
			where: [
				{column: "schema_name", op: "=", param: "SchemaName"},
				{column: "table_name", op: "=", param: "TableName"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	{
		name: "GetExternalTableByTableName"
		kind: "one"
		params: [
			{name: "tableName", type: "string"},
		]
		result: {table: "external_tables"}
		select: {
			from: "external_tables"
			where: [
				{column: "table_name", op: "=", param: "tableName"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	#ListAllOrdered & {
		name:   "ListAllExternalTables"
		_table: "external_tables"
		_order: []
		select: {
			where: [
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListExternalTables"
		_table: "external_tables"
		_params: [
			{name: "SchemaName", type: "string"},
		]
		_where: [
			{column: "schema_name", op: "=", param: "SchemaName"},
			{column: "deleted_at", op: "IS", valueSQL: "NULL"},
		]
		_order: [
			{expr: "table_name"},
		]
	},
	{
		name: "SoftDeleteExternalTable"
		kind: "exec"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
		]
		update: {
			table: "external_tables"
			set: [
				{column: "deleted_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "schema_name", op: "=", param: "SchemaName"},
				{column: "table_name", op: "=", param: "TableName"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	{
		name: "SoftDeleteExternalTablesBySchema"
		kind: "exec"
		params: [
			{name: "schemaName", type: "string"},
		]
		update: {
			table: "external_tables"
			set: [
				{column: "deleted_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "schema_name", op: "=", param: "schemaName"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	{
		name: "UpdateExternalTable"
		kind: "exec"
		params: [
			{name: "SetComment", type: "interface{}"},
			{name: "Comment", type: "string"},
			{name: "SetOwner", type: "interface{}"},
			{name: "Owner", type: "string"},
			{name: "SchemaName", type: "string"},
			{name: "TableName", type: "string"},
		]
		raw: {
			sql: "-- name: UpdateExternalTable :exec\nUPDATE external_tables\nSET comment = CASE WHEN ?1 = 1 THEN ?2 ELSE comment END,\n    owner = CASE WHEN ?3 = 1 THEN ?4 ELSE owner END,\n    updated_at = datetime('now')\nWHERE schema_name = ?5 AND table_name = ?6 AND deleted_at IS NULL"
			bind: [
				"SetComment",
				"Comment",
				"SetOwner",
				"Owner",
				"SchemaName",
				"TableName",
			]
		}
	},
]
