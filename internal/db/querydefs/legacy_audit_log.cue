package querydefs

queries: [
	#CountFiltered & {
		name:   "CountAuditLogs"
		_table: "audit_log"
		_params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Action", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
		]
		_where: [
			{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
			{column: "action", op: "=", param: "Action", optional: true},
			{column: "status", op: "=", param: "Status", optional: true},
		]
	},
	#CountFiltered & {
		name:   "CountQueryHistory"
		_table: "audit_log"
		_params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
			{name: "CreatedAtFrom", type: "sql.NullString"},
			{name: "CreatedAtTo", type: "sql.NullString"},
		]
		_where: [
			{column: "action", op: "=", valueSQL: "'QUERY'"},
			{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
			{column: "status", op: "=", param: "Status", optional: true},
			{column: "created_at", op: ">=", param: "CreatedAtFrom", optional: true},
			{column: "created_at", op: "<=", param: "CreatedAtTo", optional: true},
		]
	},
	{
		name: "InsertAuditLog"
		kind: "exec"
		params: [
			{name: "ID", type: "string"},
			{name: "PrincipalName", type: "string"},
			{name: "Action", type: "string"},
			{name: "StatementType", type: "sql.NullString"},
			{name: "OriginalSql", type: "sql.NullString"},
			{name: "RewrittenSql", type: "sql.NullString"},
			{name: "TablesAccessed", type: "sql.NullString"},
			{name: "Status", type: "string"},
			{name: "ErrorMessage", type: "sql.NullString"},
			{name: "DurationMs", type: "sql.NullInt64"},
			{name: "RowsReturned", type: "sql.NullInt64"},
		]
		insert: {
			into: "audit_log"
			columns: [
				"id",
				"principal_name",
				"action",
				"statement_type",
				"original_sql",
				"rewritten_sql",
				"tables_accessed",
				"status",
				"error_message",
				"duration_ms",
				"rows_returned",
			]
			values: [
				{param: "ID"},
				{param: "PrincipalName"},
				{param: "Action"},
				{param: "StatementType"},
				{param: "OriginalSql"},
				{param: "RewrittenSql"},
				{param: "TablesAccessed"},
				{param: "Status"},
				{param: "ErrorMessage"},
				{param: "DurationMs"},
				{param: "RowsReturned"},
			]
		}
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListAuditLogs"
		_table: "audit_log"
		_params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Action", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
		]
		_where: [
			{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
			{column: "action", op: "=", param: "Action", optional: true},
			{column: "status", op: "=", param: "Status", optional: true},
		]
		_order: [{expr: "created_at", desc: true}]
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListQueryHistory"
		_table: "audit_log"
		_params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
			{name: "CreatedAtFrom", type: "sql.NullString"},
			{name: "CreatedAtTo", type: "sql.NullString"},
		]
		_where: [
			{column: "action", op: "=", valueSQL: "'QUERY'"},
			{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
			{column: "status", op: "=", param: "Status", optional: true},
			{column: "created_at", op: ">=", param: "CreatedAtFrom", optional: true},
			{column: "created_at", op: "<=", param: "CreatedAtTo", optional: true},
		]
		_order: [{expr: "created_at", desc: true}]
	},
]
