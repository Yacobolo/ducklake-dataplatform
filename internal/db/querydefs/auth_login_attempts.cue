package querydefs

queries: [
	{
		name: "InsertAuthLoginAttempt"
		kind: "exec"
		params: [
			{name: "ID", type: "string"},
			{name: "Username", type: "sql.NullString"},
			{name: "IpAddress", type: "sql.NullString"},
			{name: "Success", type: "int64"},
			{name: "Reason", type: "sql.NullString"},
		]
		insert: {
			into: "auth_login_attempts"
			columns: [
				"id",
				"username",
				"ip_address",
				"success",
				"reason",
			]
			values: [
				{param: "ID"},
				{param: "Username"},
				{param: "IpAddress"},
				{param: "Success"},
				{param: "Reason"},
			]
		}
	},
	#CountFiltered & {
		name:   "CountRecentFailedAuthLoginAttemptsByUsername"
		_table: "auth_login_attempts"
		_params: [
			{name: "Username", type: "sql.NullString"},
			{name: "CreatedAt", type: "time.Time"},
		]
		_where: [
			{column: "username", op: "=", param: "Username"},
			{column: "success", op: "=", valueSQL: "0"},
			{column: "created_at", op: ">=", param: "CreatedAt"},
		]
	},
	#CountFiltered & {
		name:   "CountRecentFailedAuthLoginAttemptsByIP"
		_table: "auth_login_attempts"
		_params: [
			{name: "IpAddress", type: "sql.NullString"},
			{name: "CreatedAt", type: "time.Time"},
		]
		_where: [
			{column: "ip_address", op: "=", param: "IpAddress"},
			{column: "success", op: "=", valueSQL: "0"},
			{column: "created_at", op: ">=", param: "CreatedAt"},
		]
	},
]
