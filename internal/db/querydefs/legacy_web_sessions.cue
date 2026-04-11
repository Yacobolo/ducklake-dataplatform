package querydefs

#WebSessionResult: {
	row: "WebSession"
	fields: [
		{name: "ID", type: "string"},
		{name: "PrincipalID", type: "string"},
		{name: "SessionHash", type: "string"},
		{name: "AuthMethod", type: "string"},
		{name: "UserAgent", type: "sql.NullString"},
		{name: "IpAddress", type: "sql.NullString"},
		{name: "ExpiresAt", type: "time.Time"},
		{name: "IdleExpiresAt", type: "time.Time"},
		{name: "LastSeenAt", type: "time.Time"},
		{name: "RevokedAt", type: "sql.NullTime"},
		{name: "CreatedAt", type: "time.Time"},
		{name: "UpdatedAt", type: "time.Time"},
	]
}

queries: [
	#CountFiltered & {
		name:   "CountActiveWebSessions"
		_table: "web_sessions"
		_params: []
		_where: [
			{column: "revoked_at", op: "IS", valueSQL: "NULL"},
			{column: "expires_at", op: ">", valueSQL: "CURRENT_TIMESTAMP"},
			{column: "idle_expires_at", op: ">", valueSQL: "CURRENT_TIMESTAMP"},
		]
	},
	{
		name: "CreateWebSession"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "SessionHash", type: "string"},
			{name: "AuthMethod", type: "string"},
			{name: "UserAgent", type: "sql.NullString"},
			{name: "IpAddress", type: "sql.NullString"},
			{name: "ExpiresAt", type: "time.Time"},
			{name: "IdleExpiresAt", type: "time.Time"},
		]
		result: #WebSessionResult
		insert: {
			into: "web_sessions"
			columns: [
				"id",
				"principal_id",
				"session_hash",
				"auth_method",
				"user_agent",
				"ip_address",
				"expires_at",
				"idle_expires_at",
				"last_seen_at",
			]
			values: [
				{param: "ID"},
				{param: "PrincipalID"},
				{param: "SessionHash"},
				{param: "AuthMethod"},
				{param: "UserAgent"},
				{param: "IpAddress"},
				{param: "ExpiresAt"},
				{param: "IdleExpiresAt"},
				{sql: "CURRENT_TIMESTAMP"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "session_hash"},
				{expr: "auth_method"},
				{expr: "user_agent"},
				{expr: "ip_address"},
				{expr: "expires_at"},
				{expr: "idle_expires_at"},
				{expr: "last_seen_at"},
				{expr: "revoked_at"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteExpiredOrRevokedWebSessions"
		kind: "execrows"
		delete: {
			from: "web_sessions"
			where: [
				{rawSQL: "revoked_at IS NOT NULL OR expires_at <= CURRENT_TIMESTAMP OR idle_expires_at <= CURRENT_TIMESTAMP"},
			]
		}
	},
	{
		name: "GetActiveWebSessionByHash"
		kind: "one"
		params: [
			{name: "sessionHash", type: "string"},
		]
		result: #WebSessionResult
		select: {
			from: "web_sessions"
			columns: [
				{expr: "id"},
				{expr: "principal_id"},
				{expr: "session_hash"},
				{expr: "auth_method"},
				{expr: "user_agent"},
				{expr: "ip_address"},
				{expr: "expires_at"},
				{expr: "idle_expires_at"},
				{expr: "last_seen_at"},
				{expr: "revoked_at"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "session_hash", op: "=", param: "sessionHash"},
				{column: "revoked_at", op: "IS", valueSQL: "NULL"},
				{column: "expires_at", op: ">", valueSQL: "CURRENT_TIMESTAMP"},
				{column: "idle_expires_at", op: ">", valueSQL: "CURRENT_TIMESTAMP"},
			]
			limitSQL: "1"
		}
	},
	{
		name: "RevokeWebSession"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		update: {
			table: "web_sessions"
			set: [
				{column: "revoked_at", value: {sql: "CURRENT_TIMESTAMP"}},
				{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "RevokeWebSessionByHash"
		kind: "exec"
		params: [
			{name: "sessionHash", type: "string"},
		]
		update: {
			table: "web_sessions"
			set: [
				{column: "revoked_at", value: {sql: "CURRENT_TIMESTAMP"}},
				{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [
				{column: "session_hash", op: "=", param: "sessionHash"},
			]
		}
	},
	{
		name: "RevokeWebSessionsByPrincipal"
		kind: "exec"
		params: [
			{name: "principalID", type: "string"},
		]
		update: {
			table: "web_sessions"
			set: [
				{column: "revoked_at", value: {sql: "CURRENT_TIMESTAMP"}},
				{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [
				{column: "principal_id", op: "=", param: "principalID"},
				{column: "revoked_at", op: "IS", valueSQL: "NULL"},
			]
		}
	},
	{
		name: "TouchWebSession"
		kind: "exec"
		params: [
			{name: "IdleExpiresAt", type: "time.Time"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "web_sessions"
			set: [
				{column: "idle_expires_at", value: {param: "IdleExpiresAt"}},
				{column: "last_seen_at", value: {sql: "CURRENT_TIMESTAMP"}},
				{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
