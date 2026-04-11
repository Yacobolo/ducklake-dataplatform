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
	{
		name: "CountCatalogs"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "catalogs"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateCatalog"
		kind: "one"
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
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "catalogs"
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
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "metastore_type"},
				{expr: "dsn"},
				{expr: "data_path"},
				{expr: "status"},
				{expr: "status_message"},
				{expr: "is_default"},
				{expr: "comment"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteCatalog"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "catalogs"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetCatalogByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "catalogs"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "metastore_type"},
				{expr: "dsn"},
				{expr: "data_path"},
				{expr: "status"},
				{expr: "status_message"},
				{expr: "is_default"},
				{expr: "comment"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetCatalogByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "catalogs"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "metastore_type"},
				{expr: "dsn"},
				{expr: "data_path"},
				{expr: "status"},
				{expr: "status_message"},
				{expr: "is_default"},
				{expr: "comment"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "GetDefaultCatalog"
		kind: "one"
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "catalogs"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "metastore_type"},
				{expr: "dsn"},
				{expr: "data_path"},
				{expr: "status"},
				{expr: "status_message"},
				{expr: "is_default"},
				{expr: "comment"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "is_default", op: "=", valueSQL: "1"},
			]
		}
	},
	{
		name: "ListCatalogs"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "catalogs"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "metastore_type"},
				{expr: "dsn"},
				{expr: "data_path"},
				{expr: "status"},
				{expr: "status_message"},
				{expr: "is_default"},
				{expr: "comment"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
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
	{
		name: "UpdateCatalog"
		kind: "one"
		params: [
			{name: "Comment", type: "sql.NullString"},
			{name: "DataPath", type: "string"},
			{name: "Dsn", type: "string"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "Catalog"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetastoreType", type: "string"},
				{name: "Dsn", type: "string"},
				{name: "DataPath", type: "string"},
				{name: "Status", type: "string"},
				{name: "StatusMessage", type: "sql.NullString"},
				{name: "IsDefault", type: "int64"},
				{name: "Comment", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		update: {
			table: "catalogs"
			set: [
				{column: "comment", value: {param: "Comment"}, coalesceWith: true},
				{column: "data_path", value: {param: "DataPath"}, coalesceWith: true},
				{column: "dsn", value: {param: "Dsn"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "metastore_type"},
				{expr: "dsn"},
				{expr: "data_path"},
				{expr: "status"},
				{expr: "status_message"},
				{expr: "is_default"},
				{expr: "comment"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "UpdateCatalogStatus"
		kind: "exec"
		params: [
			{name: "Status", type: "string"},
			{name: "StatusMessage", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "catalogs"
			set: [
				{column: "status", value: {param: "Status"}},
				{column: "status_message", value: {param: "StatusMessage"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
