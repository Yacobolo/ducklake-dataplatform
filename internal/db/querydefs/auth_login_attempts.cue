package querydefs

queries: [
	{
		name: "InsertAuthLoginAttempt"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "ID", type: "string"},
			{name: "Username", type: "sql.NullString"},
			{name: "IpAddress", type: "sql.NullString"},
			{name: "Success", type: "int64"},
			{name: "Reason", type: "sql.NullString"},
		]
		insert: {
			into:    "auth_login_attempts"
			columns: ["id", "username", "ip_address", "success", "reason"]
			values: [
				{param: "ID"},
				{param: "Username"},
				{param: "IpAddress"},
				{param: "Success"},
				{param: "Reason"},
			]
		}
	},
	{
		name: "CountRecentFailedAuthLoginAttemptsByUsername"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "Username", type: "sql.NullString"},
			{name: "CreatedAt", type: "time.Time"},
		]
		result: {scalar: "int64"}
		select: {
			from:    "auth_login_attempts"
			columns: [{expr: "COUNT(*)"}]
			where: [
				{column: "username", op: "=", param: "Username"},
				{column: "success", op: "=", valueSQL: "0"},
				{column: "created_at", op: ">=", param: "CreatedAt"},
			]
		}
	},
	{
		name: "CountRecentFailedAuthLoginAttemptsByIP"
		kind: "one"
		paramMode: "struct"
		params: [
			{name: "IpAddress", type: "sql.NullString"},
			{name: "CreatedAt", type: "time.Time"},
		]
		result: {scalar: "int64"}
		select: {
			from:    "auth_login_attempts"
			columns: [{expr: "COUNT(*)"}]
			where: [
				{column: "ip_address", op: "=", param: "IpAddress"},
				{column: "success", op: "=", valueSQL: "0"},
				{column: "created_at", op: ">=", param: "CreatedAt"},
			]
		}
	},
]
