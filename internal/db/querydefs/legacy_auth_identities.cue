package querydefs

queries: [
	{
		name: "CreateAuthIdentity"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "Provider", type: "string"},
			{name: "Issuer", type: "sql.NullString"},
			{name: "Subject", type: "string"},
			{name: "Email", type: "sql.NullString"},
			{name: "EmailVerified", type: "int64"},
		]
		result: {
			row: "AuthIdentity"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Provider", type: "string"},
				{name: "Issuer", type: "sql.NullString"},
				{name: "Subject", type: "string"},
				{name: "Email", type: "sql.NullString"},
				{name: "EmailVerified", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		insert: {
			into: "auth_identities"
			columns: [
				"id",
				"principal_id",
				"provider",
				"issuer",
				"subject",
				"email",
				"email_verified",
			]
			values: [
				{param: "ID"},
				{param: "PrincipalID"},
				{param: "Provider"},
				{param: "Issuer"},
				{param: "Subject"},
				{param: "Email"},
				{param: "EmailVerified"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "provider"},
				{expr: "issuer"},
				{expr: "subject"},
				{expr: "email"},
				{expr: "email_verified"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteAuthIdentity"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "auth_identities"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetAuthIdentityByProviderSubject"
		kind: "one"
		params: [
			{name: "Provider", type: "string"},
			{name: "Issuer", type: "sql.NullString"},
			{name: "Subject", type: "string"},
		]
		result: {
			row: "AuthIdentity"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Provider", type: "string"},
				{name: "Issuer", type: "sql.NullString"},
				{name: "Subject", type: "string"},
				{name: "Email", type: "sql.NullString"},
				{name: "EmailVerified", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		select: {
			from: "auth_identities"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "provider"},
				{expr: "issuer"},
				{expr: "subject"},
				{expr: "email"},
				{expr: "email_verified"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "provider", op: "=", param: "Provider"},
				{column: "issuer", op: "IS", param: "Issuer"},
				{column: "subject", op: "=", param: "Subject"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "ListAuthIdentitiesByPrincipal"
		kind: "many"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			row: "AuthIdentity"
			fields: [
				{name: "ID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Provider", type: "string"},
				{name: "Issuer", type: "sql.NullString"},
				{name: "Subject", type: "string"},
				{name: "Email", type: "sql.NullString"},
				{name: "EmailVerified", type: "int64"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		select: {
			from: "auth_identities"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "provider"},
				{expr: "issuer"},
				{expr: "subject"},
				{expr: "email"},
				{expr: "email_verified"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "principal_id", op: "=", param: "principalID"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
		}
	},
]
