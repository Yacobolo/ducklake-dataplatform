package querydefs

queries: [
	{
		name: "GetSetupState"
		kind: "one"
		result: {
			row: "SetupState"
			fields: [
				{name: "ID", type: "int64"},
				{name: "SetupCompleted", type: "int64"},
				{name: "SetupCompletedAt", type: "sql.NullTime"},
				{name: "SetupCompletedBy", type: "sql.NullString"},
				{name: "BootstrapTokenHash", type: "sql.NullString"},
				{name: "BootstrapTokenExpiresAt", type: "sql.NullTime"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		select: {
			from: "setup_state"
			columns: [
				{expr: "id"},
				{expr: "setup_completed"},
				{expr: "setup_completed_at"},
				{expr: "setup_completed_by"},
				{expr: "bootstrap_token_hash"},
				{expr: "bootstrap_token_expires_at"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [{column: "id", op: "=", valueSQL: "1"}]
		}
	},
	{
		name: "SetSetupBootstrapToken"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "BootstrapTokenHash", type: "sql.NullString"},
			{name: "BootstrapTokenExpiresAt", type: "sql.NullTime"},
		]
		update: {
			table: "setup_state"
			set: [
				{column: "bootstrap_token_hash", value: {param: "BootstrapTokenHash"}},
				{column: "bootstrap_token_expires_at", value: {param: "BootstrapTokenExpiresAt"}},
				{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [{column: "id", op: "=", valueSQL: "1"}]
		}
	},
	{
		name: "ClearSetupBootstrapToken"
		kind: "exec"
		update: {
			table: "setup_state"
			set: [
				{column: "bootstrap_token_hash", value: {sql: "NULL"}},
				{column: "bootstrap_token_expires_at", value: {sql: "NULL"}},
				{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [{column: "id", op: "=", valueSQL: "1"}]
		}
	},
	{
		name: "CompleteSetupState"
		kind: "exec"
		paramMode: "single"
		params: [
			{name: "setupCompletedBy", type: "sql.NullString"},
		]
		update: {
			table: "setup_state"
			set: [
				{column: "setup_completed", value: {sql: "1"}},
				{column: "setup_completed_at", value: {sql: "CURRENT_TIMESTAMP"}},
				{column: "setup_completed_by", value: {param: "setupCompletedBy"}},
				{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
			]
			where: [{column: "id", op: "=", valueSQL: "1"}]
		}
	},
]
