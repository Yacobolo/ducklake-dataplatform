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
	{
		name: "CountPrincipals"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "principals"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
		}
	},
	{
		name: "CreatePrincipal"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Type", type: "string"},
			{name: "IsAdmin", type: "int64"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		insert: {
			into: "principals"
			columns: ["id", "name", "type", "is_admin"]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Type"},
				{param: "IsAdmin"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "type"},
				{expr: "is_admin"},
				{expr: "created_at"},
				{expr: "external_id"},
				{expr: "external_issuer"},
			]
		}
	},
	{
		name: "CreatePrincipalWithExternalID"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Type", type: "string"},
			{name: "IsAdmin", type: "int64"},
			{name: "ExternalID", type: "sql.NullString"},
			{name: "ExternalIssuer", type: "sql.NullString"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		insert: {
			into: "principals"
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
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "type"},
				{expr: "is_admin"},
				{expr: "created_at"},
				{expr: "external_id"},
				{expr: "external_issuer"},
			]
		}
	},
	{
		name: "DeletePrincipal"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "principals"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetPrincipal"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		select: {
			from: "principals"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "type"},
				{expr: "is_admin"},
				{expr: "created_at"},
				{expr: "external_id"},
				{expr: "external_issuer"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetPrincipalByExternalID"
		kind: "one"
		params: [
			{name: "ExternalIssuer", type: "sql.NullString"},
			{name: "ExternalID", type: "sql.NullString"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		select: {
			from: "principals"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "type"},
				{expr: "is_admin"},
				{expr: "created_at"},
				{expr: "external_id"},
				{expr: "external_issuer"},
			]
			where: [
				{column: "external_issuer", op: "IS", param: "ExternalIssuer"},
				{column: "external_id", op: "=", param: "ExternalID"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "GetPrincipalByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		select: {
			from: "principals"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "type"},
				{expr: "is_admin"},
				{expr: "created_at"},
				{expr: "external_id"},
				{expr: "external_issuer"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "ListPrincipals"
		kind: "many"
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		select: {
			from: "principals"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "type"},
				{expr: "is_admin"},
				{expr: "created_at"},
				{expr: "external_id"},
				{expr: "external_issuer"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
	{
		name: "ListPrincipalsPaginated"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Principal"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Type", type: "string"},
				{name: "IsAdmin", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "ExternalID", type: "sql.NullString"},
				{name: "ExternalIssuer", type: "sql.NullString"},
			]
		}
		select: {
			from: "principals"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "type"},
				{expr: "is_admin"},
				{expr: "created_at"},
				{expr: "external_id"},
				{expr: "external_issuer"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
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
