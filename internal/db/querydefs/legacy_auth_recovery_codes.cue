package querydefs

#AuthRecoveryCodeResult: {
	row: "AuthRecoveryCode"
	fields: [
		{name: "ID", type: "string"},
		{name: "PrincipalID", type: "string"},
		{name: "CodeHash", type: "string"},
		{name: "UsedAt", type: "sql.NullTime"},
		{name: "ExpiresAt", type: "time.Time"},
		{name: "CreatedAt", type: "time.Time"},
	]
}

queries: [
	{
		name: "CreateAuthRecoveryCode"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "CodeHash", type: "string"},
			{name: "ExpiresAt", type: "time.Time"},
		]
		result: #AuthRecoveryCodeResult
		insert: {
			into: "auth_recovery_codes"
			columns: [
				"id",
				"principal_id",
				"code_hash",
				"expires_at",
			]
			values: [
				{param: "ID"},
				{param: "PrincipalID"},
				{param: "CodeHash"},
				{param: "ExpiresAt"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "code_hash"},
				{expr: "used_at"},
				{expr: "expires_at"},
				{expr: "created_at"},
			]
		}
	},
	{
		name: "DeleteExpiredAuthRecoveryCodes"
		kind: "execrows"
		delete: {
			from: "auth_recovery_codes"
			where: [
				{rawSQL: "used_at IS NOT NULL OR expires_at <= CURRENT_TIMESTAMP"},
			]
		}
	},
	{
		name: "GetUnusedAuthRecoveryCodeByHash"
		kind: "one"
		params: [
			{name: "codeHash", type: "string"},
		]
		result: #AuthRecoveryCodeResult
		select: {
			from: "auth_recovery_codes"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "code_hash"},
				{expr: "used_at"},
				{expr: "expires_at"},
				{expr: "created_at"},
			]
			where: [
				{column: "code_hash", op: "=", param: "codeHash"},
				{column: "used_at", op: "IS", valueSQL: "NULL"},
				{column: "expires_at", op: ">", valueSQL: "CURRENT_TIMESTAMP"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "ListAuthRecoveryCodesByPrincipal"
		kind: "many"
		params: [
			{name: "principalID", type: "string"},
		]
		result: #AuthRecoveryCodeResult
		select: {
			from: "auth_recovery_codes"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "code_hash"},
				{expr: "used_at"},
				{expr: "expires_at"},
				{expr: "created_at"},
			]
			where: [
				{column: "principal_id", op: "=", param: "principalID"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
		}
	},
	{
		name: "MarkAuthRecoveryCodeUsed"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		update: {
			table: "auth_recovery_codes"
			set: [
				{column: "used_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
]
