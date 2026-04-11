package querydefs

queries: [
	{
		name: "CountAPIKeysForPrincipal"
		kind: "one"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "api_keys"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "principal_id", op: "=", param: "principalID"},
			]
		}
	},
	{
		name: "CountAllAPIKeys"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "api_keys"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
		}
	},
	{
		name: "CreateAPIKey"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "KeyHash", type: "string"},
			{name: "KeyPrefix", type: "sql.NullString"},
			{name: "PrincipalID", type: "string"},
			{name: "Name", type: "string"},
			{name: "ExpiresAt", type: "sql.NullString"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		insert: {
			into: "api_keys"
			columns: [
				"id",
				"key_hash",
				"key_prefix",
				"principal_id",
				"name",
				"expires_at",
			]
			values: [
				{param: "ID"},
				{param: "KeyHash"},
				{param: "KeyPrefix"},
				{param: "PrincipalID"},
				{param: "Name"},
				{param: "ExpiresAt"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "key_hash"},
				{expr: "principal_id"},
				{expr: "name"},
				{expr: "expires_at"},
				{expr: "created_at"},
				{expr: "key_prefix"},
			]
		}
	},
	{
		name: "DeleteAPIKey"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "api_keys"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "DeleteExpiredKeys"
		kind: "execresult"
		delete: {
			from: "api_keys"
			where: [
				{column: "expires_at", op: "IS NOT", valueSQL: "NULL"},
				{column: "expires_at", op: "<=", valueSQL: "datetime('now', 'localtime')"},
			]
		}
	},
	{
		name: "GetAPIKeyByHash"
		kind: "one"
		params: [
			{name: "keyHash", type: "string"},
		]
		result: {
			row: "GetAPIKeyByHashRow"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
				{name: "PrincipalName", type: "string"},
			]
		}
		select: {
			from: "api_keys"
			alias: "ak"
			columns: [
				{expr: "ak.id"},
				{expr: "ak.key_hash"},
				{expr: "ak.principal_id"},
				{expr: "ak.name"},
				{expr: "ak.expires_at"},
				{expr: "ak.created_at"},
				{expr: "ak.key_prefix"},
				{expr: "p.name", alias: "principal_name"},
			]
			joins: [
				{type: "JOIN", table: "principals", alias: "p", on: "ak.principal_id = p.id"},
			]
			where: [
				{column: "ak.key_hash", op: "=", param: "keyHash"},
				{rawSQL: "(ak.expires_at IS NULL OR ak.expires_at > datetime('now', 'localtime'))"},
			]
		}
	},
	{
		name: "GetAPIKeyByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		select: {
			from: "api_keys"
			columns: [
				{expr: "id"},
				{expr: "key_hash"},
				{expr: "principal_id"},
				{expr: "name"},
				{expr: "expires_at"},
				{expr: "created_at"},
				{expr: "key_prefix"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListAPIKeysForPrincipal"
		kind: "many"
		params: [
			{name: "principalID", type: "string"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		select: {
			from: "api_keys"
			columns: [
				{expr: "id"},
				{expr: "key_hash"},
				{expr: "principal_id"},
				{expr: "name"},
				{expr: "expires_at"},
				{expr: "created_at"},
				{expr: "key_prefix"},
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
		name: "ListAPIKeysForPrincipalPaginated"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		select: {
			from: "api_keys"
			columns: [
				{expr: "id"},
				{expr: "key_hash"},
				{expr: "principal_id"},
				{expr: "name"},
				{expr: "expires_at"},
				{expr: "created_at"},
				{expr: "key_prefix"},
			]
			where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "ListAllAPIKeysPaginated"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ApiKey"
			fields: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
			]
		}
		select: {
			from: "api_keys"
			columns: [
				{expr: "id"},
				{expr: "key_hash"},
				{expr: "principal_id"},
				{expr: "name"},
				{expr: "expires_at"},
				{expr: "created_at"},
				{expr: "key_prefix"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
]
