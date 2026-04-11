package querydefs

queries: [
	{
		name: "ClearDefaultCatalog"
		kind: "exec"
		update: {
			table: "catalogs"
			set: [
				{column: "is_default", value: {sql: "0"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "is_default", op: "=", valueSQL: "1"},
			]
		}
	},
	#CountAll & {
		name:   "CountCatalogs"
		_table: "catalogs"
	},
	#InsertReturningTable & {
		name:   "CreateCatalog"
		_table: "catalogs"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "MetastoreType", type: "string"},
			{name: "Dsn", type: "string"},
			{name: "DataPath", type: "string"},
			{name: "Status", type: "string"},
			{name: "StatusMessage", type: "sql.NullString"},
			{name: "IsDefault", type: "int64"},
			{name: "Comment", type: "sql.NullString"},
		]
		insert: {
			columns: [
				"id",
				"name",
				"metastore_type",
				"dsn",
				"data_path",
				"status",
				"status_message",
				"is_default",
				"comment",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "MetastoreType"},
				{param: "Dsn"},
				{param: "DataPath"},
				{param: "Status"},
				{param: "StatusMessage"},
				{param: "IsDefault"},
				{param: "Comment"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteCatalog"
		_table: "catalogs"
	},
	#GetByID & {
		name:   "GetCatalogByID"
		_table: "catalogs"
	},
	#GetByStringField & {
		name:   "GetCatalogByName"
		_table: "catalogs"
		_field: "name"
		_param: "name"
	},
	{
		name: "GetDefaultCatalog"
		kind: "one"
		result: {table: "catalogs"}
		select: {
			from: "catalogs"
			where: [
				{column: "is_default", op: "=", valueSQL: "1"},
			]
		}
	},
	#ListPaginatedOrdered & {
		name:   "ListCatalogs"
		_table: "catalogs"
		_order: [
			{expr: "name"},
		]
	},
	{
		name: "SetDefaultCatalog"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		update: {
			table: "catalogs"
			set: [
				{column: "is_default", value: {sql: "1"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	#UpdateByIDTouch & {
		name:   "UpdateCatalog"
		_table: "catalogs"
		_kind:  "one"
		params: [
			{name: "Comment", type: "sql.NullString"},
			{name: "DataPath", type: "string"},
			{name: "Dsn", type: "string"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "comment", value: {param: "Comment"}, coalesceWith: true},
			{column: "data_path", value: {param: "DataPath"}, coalesceWith: true},
			{column: "dsn", value: {param: "Dsn"}, coalesceWith: true},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateCatalogStatus"
		_table: "catalogs"
		params: [
			{name: "Status", type: "string"},
			{name: "StatusMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "status", value: {param: "Status"}},
			{column: "status_message", value: {param: "StatusMessage"}},
		]
	},
]
