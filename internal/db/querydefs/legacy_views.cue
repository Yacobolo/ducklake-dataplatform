package querydefs

queries: [
	{
		name: "CountViews"
		kind: "one"
		params: [
			{name: "schemaID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "views"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "schema_id", op: "=", param: "schemaID"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	{
		name: "CreateView"
		kind: "one"
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
		result: {
			row: "View"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ViewDefinition", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "SourceTables", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		insert: {
			into: "views"
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
			returningColumns: [
				{expr: "id"},
				{expr: "schema_id"},
				{expr: "name"},
				{expr: "view_definition"},
				{expr: "comment"},
				{expr: "properties"},
				{expr: "owner"},
				{expr: "source_tables"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "deleted_at"},
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
		result: {
			row: "View"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ViewDefinition", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "SourceTables", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		select: {
			from: "views"
			columns: [
				{expr: "id"},
				{expr: "schema_id"},
				{expr: "name"},
				{expr: "view_definition"},
				{expr: "comment"},
				{expr: "properties"},
				{expr: "owner"},
				{expr: "source_tables"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "deleted_at"},
			]
			where: [
				{column: "schema_id", op: "=", param: "SchemaID"},
				{column: "name", op: "=", param: "Name"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	{
		name: "ListViews"
		kind: "many"
		params: [
			{name: "SchemaID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "View"
			fields: [
				{name: "ID", type: "string"},
				{name: "SchemaID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ViewDefinition", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "SourceTables", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		select: {
			from: "views"
			columns: [
				{expr: "id"},
				{expr: "schema_id"},
				{expr: "name"},
				{expr: "view_definition"},
				{expr: "comment"},
				{expr: "properties"},
				{expr: "owner"},
				{expr: "source_tables"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "deleted_at"},
			]
			where: [
				{column: "schema_id", op: "=", param: "SchemaID"},
				{column: "deleted_at", op: "IS", valueSQL: "NULL"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
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
