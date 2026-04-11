package querydefs

queries: [
	{
		name: "CountGitRepos"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "git_repos"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateGitRepo"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Url", type: "string"},
			{name: "Branch", type: "string"},
			{name: "Path", type: "string"},
			{name: "AuthToken", type: "string"},
			{name: "WebhookSecret", type: "sql.NullString"},
			{name: "Owner", type: "string"},
		]
		result: {
			row: "GitRepo"
			fields: [
				{name: "ID", type: "string"},
				{name: "Url", type: "string"},
				{name: "Branch", type: "string"},
				{name: "Path", type: "string"},
				{name: "AuthToken", type: "string"},
				{name: "WebhookSecret", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "LastSyncAt", type: "sql.NullString"},
				{name: "LastCommit", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "git_repos"
			columns: [
				"id",
				"url",
				"branch",
				"path",
				"auth_token",
				"webhook_secret",
				"owner",
			]
			values: [
				{param: "ID"},
				{param: "Url"},
				{param: "Branch"},
				{param: "Path"},
				{param: "AuthToken"},
				{param: "WebhookSecret"},
				{param: "Owner"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "url"},
				{expr: "branch"},
				{expr: "path"},
				{expr: "auth_token"},
				{expr: "webhook_secret"},
				{expr: "owner"},
				{expr: "last_sync_at"},
				{expr: "last_commit"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteGitRepo"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "git_repos"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetGitRepo"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "GitRepo"
			fields: [
				{name: "ID", type: "string"},
				{name: "Url", type: "string"},
				{name: "Branch", type: "string"},
				{name: "Path", type: "string"},
				{name: "AuthToken", type: "string"},
				{name: "WebhookSecret", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "LastSyncAt", type: "sql.NullString"},
				{name: "LastCommit", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "git_repos"
			columns: [
				{expr: "id"},
				{expr: "url"},
				{expr: "branch"},
				{expr: "path"},
				{expr: "auth_token"},
				{expr: "webhook_secret"},
				{expr: "owner"},
				{expr: "last_sync_at"},
				{expr: "last_commit"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListGitRepos"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "GitRepo"
			fields: [
				{name: "ID", type: "string"},
				{name: "Url", type: "string"},
				{name: "Branch", type: "string"},
				{name: "Path", type: "string"},
				{name: "AuthToken", type: "string"},
				{name: "WebhookSecret", type: "sql.NullString"},
				{name: "Owner", type: "string"},
				{name: "LastSyncAt", type: "sql.NullString"},
				{name: "LastCommit", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "git_repos"
			columns: [
				{expr: "id"},
				{expr: "url"},
				{expr: "branch"},
				{expr: "path"},
				{expr: "auth_token"},
				{expr: "webhook_secret"},
				{expr: "owner"},
				{expr: "last_sync_at"},
				{expr: "last_commit"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "UpdateGitRepoSyncStatus"
		kind: "exec"
		params: [
			{name: "LastCommit", type: "sql.NullString"},
			{name: "LastSyncAt", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "git_repos"
			set: [
				{column: "last_commit", value: {param: "LastCommit"}},
				{column: "last_sync_at", value: {param: "LastSyncAt"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
