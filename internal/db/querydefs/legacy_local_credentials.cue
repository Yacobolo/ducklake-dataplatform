package querydefs

#LocalCredentialResult: {
	row: "LocalCredential"
	fields: [
		{name: "PrincipalID", type: "string"},
		{name: "Username", type: "string"},
		{name: "PasswordHash", type: "string"},
		{name: "PasswordChangedAt", type: "time.Time"},
		{name: "MustChangePassword", type: "int64"},
		{name: "CreatedAt", type: "time.Time"},
		{name: "UpdatedAt", type: "time.Time"},
	]
}

queries: [
	{
		name: "DeleteLocalCredential"
		kind: "exec"
		params: [
			{name: "principalID", type: "string"},
		]
		delete: {
			from: "local_credentials"
			where: [
				{column: "principal_id", op: "=", param: "principalID"},
			]
		}
	},
	{
		name: "GetLocalCredentialByPrincipalID"
		kind: "one"
		params: [
			{name: "principalID", type: "string"},
		]
		result: #LocalCredentialResult
		select: {
			from: "local_credentials"
			columns: [
				{expr: "principal_id"},
				{expr: "username"},
				{expr: "password_hash"},
				{expr: "password_changed_at"},
				{expr: "must_change_password"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "principal_id", op: "=", param: "principalID"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "GetLocalCredentialByUsername"
		kind: "one"
		params: [
			{name: "username", type: "string"},
		]
		result: #LocalCredentialResult
		select: {
			from: "local_credentials"
			columns: [
				{expr: "principal_id"},
				{expr: "username"},
				{expr: "password_hash"},
				{expr: "password_changed_at"},
				{expr: "must_change_password"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "username", op: "=", param: "username"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "UpsertLocalCredential"
		kind: "exec"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "Username", type: "string"},
			{name: "PasswordHash", type: "string"},
			{name: "MustChangePassword", type: "int64"},
		]
		insert: {
			into: "local_credentials"
			columns: [
				"principal_id",
				"username",
				"password_hash",
				"password_changed_at",
				"must_change_password",
				"updated_at",
			]
			values: [
				{param: "PrincipalID"},
				{param: "Username"},
				{param: "PasswordHash"},
				{sql: "CURRENT_TIMESTAMP"},
				{param: "MustChangePassword"},
				{sql: "CURRENT_TIMESTAMP"},
			]
			conflict: {
				targets: [
					"principal_id",
				]
				doUpdate: [
					{column: "username", value: {sql: "excluded.username"}},
					{column: "password_hash", value: {sql: "excluded.password_hash"}},
					{column: "password_changed_at", value: {sql: "CURRENT_TIMESTAMP"}},
					{column: "must_change_password", value: {sql: "excluded.must_change_password"}},
					{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
				]
			}
		}
	},
]
