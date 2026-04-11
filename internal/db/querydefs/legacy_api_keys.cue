package querydefs

apiKeys: {
	table: "api_keys"
	order: [{expr: "created_at", desc: true}]
	byPrincipalParams: [{name: "principalID", type: "string"}]
	byPrincipalWhere:  [{column: "principal_id", op: "=", param: "principalID"}]

	#HashLookupRow: {
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

	queries: {
		countForPrincipal: #CountFiltered & {
			name: "CountAPIKeysForPrincipal"
			_table:  apiKeys.table
			_params: apiKeys.byPrincipalParams
			_where:  apiKeys.byPrincipalWhere
		}

		countAll: #CountAll & {
			name: "CountAllAPIKeys"
			_table: apiKeys.table
		}

		create: #InsertReturningTable & {
			name: "CreateAPIKey"
			_table: apiKeys.table
			params: [
				{name: "ID", type: "string"},
				{name: "KeyHash", type: "string"},
				{name: "KeyPrefix", type: "sql.NullString"},
				{name: "PrincipalID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ExpiresAt", type: "sql.NullString"},
			]
			insert: {
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
			}
		}

		deleteByID: #DeleteByID & {
			name: "DeleteAPIKey"
			_table: apiKeys.table
		}

		deleteExpired: {
			name: "DeleteExpiredKeys"
			kind: "execresult"
			delete: {
				from: apiKeys.table
				where: [
					{column: "expires_at", op: "IS NOT", valueSQL: "NULL"},
					{column: "expires_at", op: "<=", valueSQL: "datetime('now', 'localtime')"},
				]
			}
		}

		getByHash: {
			name: "GetAPIKeyByHash"
			kind: "one"
			params: [{name: "keyHash", type: "string"}]
			result: #HashLookupRow
			select: {
				from:  apiKeys.table
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
		}

		getByID: #GetByID & {
			name: "GetAPIKeyByID"
			_table: apiKeys.table
		}

		listByPrincipal: {
			name:   "ListAPIKeysForPrincipal"
			kind:   "many"
			params: apiKeys.byPrincipalParams
			result: {table: apiKeys.table}
			select: {
				from:    apiKeys.table
				where:   apiKeys.byPrincipalWhere
				orderBy: apiKeys.order
			}
		}

		listByPrincipalPaginated: #ListFilteredPaginatedOrdered & {
			name:   "ListAPIKeysForPrincipalPaginated"
			_table: apiKeys.table
			_order: apiKeys.order
			_params: [
				{name: "PrincipalID", type: "string"},
			]
			_where: [
				{column: "principal_id", op: "=", param: "PrincipalID"},
			]
		}

		listAllPaginated: #ListPaginatedOrdered & {
			name: "ListAllAPIKeysPaginated"
			_table: apiKeys.table
			_order: apiKeys.order
		}
	}
}

queries: [
	apiKeys.queries.countForPrincipal,
	apiKeys.queries.countAll,
	apiKeys.queries.create,
	apiKeys.queries.deleteByID,
	apiKeys.queries.deleteExpired,
	apiKeys.queries.getByHash,
	apiKeys.queries.getByID,
	apiKeys.queries.listByPrincipal,
	apiKeys.queries.listByPrincipalPaginated,
	apiKeys.queries.listAllPaginated,
]
