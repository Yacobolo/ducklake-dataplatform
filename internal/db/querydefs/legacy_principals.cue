package querydefs

queries: [
	{
		name: "BindExternalID"
		kind: "exec"
		params: [
			{name: "ExternalID", type: "sql.NullString"},
			{name: "ExternalIssuer", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "principals"
			set: [
				{column: "external_id", value: {param: "ExternalID"}},
				{column: "external_issuer", value: {param: "ExternalIssuer"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	#CountAll & {
		name:   "CountPrincipals"
		_table: "principals"
	},
	#InsertReturningTable & {
		name:   "CreatePrincipal"
		_table: "principals"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Type", type: "string"},
			{name: "IsAdmin", type: "int64"},
		]
		insert: {
			columns: [
				"id",
				"name",
				"type",
				"is_admin",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Type"},
				{param: "IsAdmin"},
			]
		}
	},
	#InsertReturningTable & {
		name:   "CreatePrincipalWithExternalID"
		_table: "principals"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Type", type: "string"},
			{name: "IsAdmin", type: "int64"},
			{name: "ExternalID", type: "sql.NullString"},
			{name: "ExternalIssuer", type: "sql.NullString"},
		]
		insert: {
			columns: [
				"id",
				"name",
				"type",
				"is_admin",
				"external_id",
				"external_issuer",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Type"},
				{param: "IsAdmin"},
				{param: "ExternalID"},
				{param: "ExternalIssuer"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeletePrincipal"
		_table: "principals"
	},
	#GetByID & {
		name:   "GetPrincipal"
		_table: "principals"
	},
	{
		name: "GetPrincipalByExternalID"
		kind: "one"
		params: [
			{name: "ExternalIssuer", type: "sql.NullString"},
			{name: "ExternalID", type: "sql.NullString"},
		]
		result: {table: "principals"}
		select: {
			from: "principals"
			where: [
				{column: "external_issuer", op: "IS", param: "ExternalIssuer"},
				{column: "external_id", op: "=", param: "ExternalID"},
			]
			limitSQL: "1"
		}
	},
	#GetByStringField & {
		name:   "GetPrincipalByName"
		_table: "principals"
		_field: "name"
		_param: "name"
	},
	#ListAllOrdered & {
		name:   "ListPrincipals"
		_table: "principals"
		_order: [
			{expr: "name"},
		]
	},
	#ListPaginatedOrdered & {
		name:   "ListPrincipalsPaginated"
		_table: "principals"
		_order: [
			{expr: "id"},
		]
	},
	{
		name: "SetAdmin"
		kind: "exec"
		params: [
			{name: "IsAdmin", type: "int64"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "principals"
			set: [
				{column: "is_admin", value: {param: "IsAdmin"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
