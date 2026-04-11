package querydefs

queries: [
	{
		name: "CountExternalLocations"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "external_locations"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateExternalLocation"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Url", type: "string"},
			{name: "CredentialName", type: "string"},
			{name: "StorageType", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ReadOnly", type: "int64"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "external_locations"
			columns: [
				"id",
				"name",
				"url",
				"credential_name",
				"storage_type",
				"comment",
				"owner",
				"read_only",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Url"},
				{param: "CredentialName"},
				{param: "StorageType"},
				{param: "Comment"},
				{param: "Owner"},
				{param: "ReadOnly"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "credential_name"},
				{expr: "storage_type"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "read_only"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteExternalLocation"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "external_locations"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetExternalLocation"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "external_locations"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "credential_name"},
				{expr: "storage_type"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "read_only"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetExternalLocationByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "external_locations"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "credential_name"},
				{expr: "storage_type"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "read_only"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "ListExternalLocations"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ExternalLocation"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Url", type: "string"},
				{name: "CredentialName", type: "string"},
				{name: "StorageType", type: "string"},
				{name: "Comment", type: "string"},
				{name: "Owner", type: "string"},
				{name: "ReadOnly", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "external_locations"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "url"},
				{expr: "credential_name"},
				{expr: "storage_type"},
				{expr: "comment"},
				{expr: "owner"},
				{expr: "read_only"},
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
		name: "UpdateExternalLocation"
		kind: "exec"
		params: [
			{name: "Url", type: "string"},
			{name: "CredentialName", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ReadOnly", type: "int64"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "external_locations"
			set: [
				{column: "url", value: {param: "Url"}, coalesceWith: true},
				{column: "credential_name", value: {param: "CredentialName"}, coalesceWith: true},
				{column: "comment", value: {param: "Comment"}, coalesceWith: true},
				{column: "owner", value: {param: "Owner"}, coalesceWith: true},
				{column: "read_only", value: {param: "ReadOnly"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
