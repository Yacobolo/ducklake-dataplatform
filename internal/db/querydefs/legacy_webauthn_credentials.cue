package querydefs

#WebauthnCredentialResult: {
	row: "WebauthnCredential"
	fields: [
		{name: "ID", type: "string"},
		{name: "PrincipalID", type: "string"},
		{name: "CredentialID", type: "string"},
		{name: "PublicKey", type: "string"},
		{name: "SignCount", type: "int64"},
		{name: "Transports", type: "sql.NullString"},
		{name: "BackupEligible", type: "int64"},
		{name: "BackupState", type: "int64"},
		{name: "CreatedAt", type: "time.Time"},
		{name: "LastUsedAt", type: "sql.NullTime"},
	]
}

queries: [
	{
		name: "CreateWebauthnCredential"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "CredentialID", type: "string"},
			{name: "PublicKey", type: "string"},
			{name: "SignCount", type: "int64"},
			{name: "Transports", type: "sql.NullString"},
			{name: "BackupEligible", type: "int64"},
			{name: "BackupState", type: "int64"},
		]
		result: #WebauthnCredentialResult
		insert: {
			into: "webauthn_credentials"
			columns: [
				"id",
				"principal_id",
				"credential_id",
				"public_key",
				"sign_count",
				"transports",
				"backup_eligible",
				"backup_state",
			]
			values: [
				{param: "ID"},
				{param: "PrincipalID"},
				{param: "CredentialID"},
				{param: "PublicKey"},
				{param: "SignCount"},
				{param: "Transports"},
				{param: "BackupEligible"},
				{param: "BackupState"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "credential_id"},
				{expr: "public_key"},
				{expr: "sign_count"},
				{expr: "transports"},
				{expr: "backup_eligible"},
				{expr: "backup_state"},
				{expr: "created_at"},
				{expr: "last_used_at"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteWebauthnCredential"
		_table: "webauthn_credentials"
	},
	{
		name: "GetWebauthnCredentialByCredentialID"
		kind: "one"
		params: [
			{name: "credentialID", type: "string"},
		]
		result: #WebauthnCredentialResult
		select: {
			from: "webauthn_credentials"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "credential_id"},
				{expr: "public_key"},
				{expr: "sign_count"},
				{expr: "transports"},
				{expr: "backup_eligible"},
				{expr: "backup_state"},
				{expr: "created_at"},
				{expr: "last_used_at"},
			]
			where: [
				{column: "credential_id", op: "=", param: "credentialID"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "ListWebauthnCredentialsByPrincipal"
		kind: "many"
		params: [
			{name: "principalID", type: "string"},
		]
		result: #WebauthnCredentialResult
		select: {
			from: "webauthn_credentials"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "credential_id"},
				{expr: "public_key"},
				{expr: "sign_count"},
				{expr: "transports"},
				{expr: "backup_eligible"},
				{expr: "backup_state"},
				{expr: "created_at"},
				{expr: "last_used_at"},
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
		name: "UpdateWebauthnCredentialCounter"
		kind: "exec"
		params: [
			{name: "SignCount", type: "int64"},
			{name: "CredentialID", type: "string"},
		]
		update: {
			table: "webauthn_credentials"
			set: [
				{column: "sign_count", value: {param: "SignCount"}},
				{column: "last_used_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [
				{column: "credential_id", op: "=", param: "CredentialID"},
			]
		}
	},
]
