package querydefs

queries: [
	{
		name: "DeleteCatalogMetadataByTypeAndName"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		delete: {
			from: "catalog_metadata"
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_name", op: "=", param: "SecurableName"},
			]
		}
	},
	{
		name: "DeleteCatalogMetadataByTypeAndPattern"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		delete: {
			from: "catalog_metadata"
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_name", op: "LIKE", param: "SecurableName"},
			]
		}
	},
	{
		name: "GetCatalogMetadata"
		kind: "one"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		result: {
			row: "CatalogMetadatum"
			fields: [
				{name: "SecurableType", type: "string"},
				{name: "SecurableName", type: "string"},
				{name: "Comment", type: "sql.NullString"},
				{name: "Properties", type: "sql.NullString"},
				{name: "Owner", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "DeletedAt", type: "sql.NullString"},
			]
		}
		select: {
			from: "catalog_metadata"
			columns: [
				{expr: "securable_type"},
				{expr: "securable_name"},
				{expr: "comment"},
				{expr: "properties"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "deleted_at"},
			]
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_name", op: "=", param: "SecurableName"},
			]
		}
	},
	{
		name: "SoftDeleteCatalogMetadata"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		update: {
			table: "catalog_metadata"
			set: [
				{column: "deleted_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_name", op: "=", param: "SecurableName"},
			]
		}
	},
	{
		name: "SoftDeleteCatalogMetadataByPattern"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
		]
		update: {
			table: "catalog_metadata"
			set: [
				{column: "deleted_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "securable_type", op: "=", param: "SecurableType"},
				{column: "securable_name", op: "LIKE", param: "SecurableName"},
			]
		}
	},
	{
		name: "UpsertCatalogMetadata"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Properties", type: "sql.NullString"},
			{name: "Owner", type: "sql.NullString"},
		]
		insert: {
			into: "catalog_metadata"
			columns: [
				"securable_type",
				"securable_name",
				"comment",
				"properties",
				"owner",
			]
			values: [
				{param: "SecurableType"},
				{param: "SecurableName"},
				{param: "Comment"},
				{param: "Properties"},
				{param: "Owner"},
			]
			conflict: {
				targets: ["securable_type", "securable_name"]
				doUpdate: [
					{column: "comment", value: {sql: "COALESCE(excluded.comment, comment)"}},
					{column: "properties", value: {sql: "COALESCE(excluded.properties, properties)"}},
					{column: "owner", value: {sql: "COALESCE(excluded.owner, owner)"}},
					{column: "deleted_at", value: {sql: "NULL"}},
					{column: "updated_at", value: {sql: "datetime('now')"}},
				]
			}
		}
	},
]
