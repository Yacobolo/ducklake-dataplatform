package querydefs

queries: [
	#CountFiltered & {
		name:   "CountVolumes"
		_table: "volumes"
		_params: [
			{name: "schemaName", type: "string"},
		]
		_where: [
			{column: "schema_name", op: "=", param: "schemaName"},
		]
	},
	#InsertReturningTable & {
		name:   "CreateVolume"
		_table: "volumes"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeleteVolume"
		_table: "volumes"
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
	#GetByTwoStringFields & {
		name:    "GetVolumeByName"
		_table:  "volumes"
		_field1: "schema_name"
		_param1: "SchemaName"
		_field2: "name"
		_param2: "Name"
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListVolumes"
		_table: "volumes"
		_params: [
			{name: "SchemaName", type: "string"},
		]
		_where: [
			{column: "schema_name", op: "=", param: "SchemaName"},
		]
		_order: [
			{expr: "name"},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateVolume"
		_table: "volumes"
		params: [
			{name: "Name", type: "string"},
			{name: "StorageLocation", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "name", value: {param: "Name"}, coalesceWith: true},
			{column: "storage_location", value: {param: "StorageLocation"}, coalesceWith: true},
			{column: "comment", value: {param: "Comment"}, coalesceWith: true},
			{column: "owner", value: {param: "Owner"}, coalesceWith: true},
		]
	},
]
