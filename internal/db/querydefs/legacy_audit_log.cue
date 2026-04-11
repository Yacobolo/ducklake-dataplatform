package querydefs

queries: [
	{
		name: "CountAuditLogs"
		kind: "one"
		params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Action", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from: "audit_log"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
				{column: "action", op: "=", param: "Action", optional: true},
				{column: "status", op: "=", param: "Status", optional: true},
			]
		}
	},
	{
		name: "CountQueryHistory"
		kind: "one"
		params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
			{name: "CreatedAtFrom", type: "sql.NullString"},
			{name: "CreatedAtTo", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from: "audit_log"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "action", op: "=", valueSQL: "'QUERY'"},
				{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
				{column: "status", op: "=", param: "Status", optional: true},
				{column: "created_at", op: ">=", param: "CreatedAtFrom", optional: true},
				{column: "created_at", op: "<=", param: "CreatedAtTo", optional: true},
			]
		}
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
	{
		name: "ListAuditLogs"
		kind: "many"
		params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Action", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "AuditLog"
			fields: [
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
				{name: "CreatedAt", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "audit_log"
			columns: [
				{expr: "id"},
				{expr: "principal_name"},
				{expr: "\"action\""},
				{expr: "statement_type"},
				{expr: "original_sql"},
				{expr: "rewritten_sql"},
				{expr: "tables_accessed"},
				{expr: "status"},
				{expr: "error_message"},
				{expr: "duration_ms"},
				{expr: "created_at"},
				{expr: "rows_returned"},
			]
			where: [
				{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
				{column: "action", op: "=", param: "Action", optional: true},
				{column: "status", op: "=", param: "Status", optional: true},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "ListQueryHistory"
		kind: "many"
		params: [
			{name: "PrincipalName", type: "sql.NullString"},
			{name: "Status", type: "sql.NullString"},
			{name: "CreatedAtFrom", type: "sql.NullString"},
			{name: "CreatedAtTo", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "AuditLog"
			fields: [
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
				{name: "CreatedAt", type: "string"},
				{name: "RowsReturned", type: "sql.NullInt64"},
			]
		}
		select: {
			from: "audit_log"
			columns: [
				{expr: "id"},
				{expr: "principal_name"},
				{expr: "\"action\""},
				{expr: "statement_type"},
				{expr: "original_sql"},
				{expr: "rewritten_sql"},
				{expr: "tables_accessed"},
				{expr: "status"},
				{expr: "error_message"},
				{expr: "duration_ms"},
				{expr: "created_at"},
				{expr: "rows_returned"},
			]
			where: [
				{column: "action", op: "=", valueSQL: "'QUERY'"},
				{column: "principal_name", op: "=", param: "PrincipalName", optional: true},
				{column: "status", op: "=", param: "Status", optional: true},
				{column: "created_at", op: ">=", param: "CreatedAtFrom", optional: true},
				{column: "created_at", op: "<=", param: "CreatedAtTo", optional: true},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
]
