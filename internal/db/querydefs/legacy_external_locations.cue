package querydefs

queries: [
	#CountAll & {
		name:   "CountExternalLocations"
		_table: "external_locations"
	},
	#InsertReturningTable & {
		name:   "CreateExternalLocation"
		_table: "external_locations"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeleteExternalLocation"
		_table: "external_locations"
	},
	#GetByID & {
		name:   "GetExternalLocation"
		_table: "external_locations"
	},
	#GetByStringField & {
		name:   "GetExternalLocationByName"
		_table: "external_locations"
		_field: "name"
		_param: "name"
	},
	#ListPaginatedOrdered & {
		name:   "ListExternalLocations"
		_table: "external_locations"
		_order: [
			{expr: "name"},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateExternalLocation"
		_table: "external_locations"
		params: [
			{name: "Url", type: "string"},
			{name: "CredentialName", type: "string"},
			{name: "Comment", type: "string"},
			{name: "Owner", type: "string"},
			{name: "ReadOnly", type: "int64"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "url", value: {param: "Url"}, coalesceWith: true},
			{column: "credential_name", value: {param: "CredentialName"}, coalesceWith: true},
			{column: "comment", value: {param: "Comment"}, coalesceWith: true},
			{column: "owner", value: {param: "Owner"}, coalesceWith: true},
			{column: "read_only", value: {param: "ReadOnly"}, coalesceWith: true},
		]
	},
]
