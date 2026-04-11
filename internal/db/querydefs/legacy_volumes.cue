package querydefs

queries: [
	{
		name: "CountVolumes"
		kind: "one"
		params: [
			{name: "schemaName", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "volumes"
			columns: [
				{expr: "COUNT(*)"},
			]
			where: [
				{column: "schema_name", op: "=", param: "schemaName"},
			]
		}
	},
	{
		name: "CreateVolume"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "SchemaName", type: "string"},
			{name: "CatalogName", type: "string"},
			{name: "VolumeType", type: "string"},
			{name: "StorageLocation", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "Volume"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "VolumeType", type: "string"},
				{name: "StorageLocation", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "volumes"
			columns: [
				"id",
				"name",
				"schema_name",
				"catalog_name",
				"volume_type",
				"storage_location",
				"comment",
				"owner",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "SchemaName"},
				{param: "CatalogName"},
				{param: "VolumeType"},
				{param: "StorageLocation"},
				{param: "Comment"},
				{param: "Owner"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "schema_name"},
				{expr: "catalog_name"},
				{expr: "volume_type"},
				{expr: "storage_location"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteVolume"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "volumes"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "DeleteVolumesBySchema"
		kind: "exec"
		params: [
			{name: "schemaName", type: "string"},
		]
		delete: {
			from: "volumes"
			where: [
				{column: "schema_name", op: "=", param: "schemaName"},
			]
		}
	},
	{
		name: "GetVolumeByName"
		kind: "one"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "Volume"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "VolumeType", type: "string"},
				{name: "StorageLocation", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "volumes"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "schema_name"},
				{expr: "catalog_name"},
				{expr: "volume_type"},
				{expr: "storage_location"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "schema_name", op: "=", param: "SchemaName"},
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
	{
		name: "ListVolumes"
		kind: "many"
		params: [
			{name: "SchemaName", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Volume"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "SchemaName", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "VolumeType", type: "string"},
				{name: "StorageLocation", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "volumes"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "schema_name"},
				{expr: "catalog_name"},
				{expr: "volume_type"},
				{expr: "storage_location"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "schema_name", op: "=", param: "SchemaName"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "UpdateVolume"
		kind: "exec"
		params: [
			{name: "Name", type: "string"},
			{name: "StorageLocation", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "volumes"
			set: [
				{column: "name", value: {param: "Name"}, coalesceWith: true},
				{column: "storage_location", value: {param: "StorageLocation"}, coalesceWith: true},
				{column: "comment", value: {param: "Comment"}, coalesceWith: true},
				{column: "owner", value: {param: "Owner"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
